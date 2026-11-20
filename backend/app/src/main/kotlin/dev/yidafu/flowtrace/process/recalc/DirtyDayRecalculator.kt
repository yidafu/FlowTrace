package dev.yidafu.flowtrace.process.recalc

import dev.yidafu.flowtrace.process.config.ProcessorProperties
import dev.yidafu.flowtrace.process.segment.SegmentBuilder
import dev.yidafu.flowtrace.process.store.RecalcGuard
import dev.yidafu.flowtrace.shared.model.SegmentStatus
import dev.yidafu.flowtrace.shared.model.TracePoint
import dev.yidafu.flowtrace.shared.model.TraceSegmentDraft
import dev.yidafu.flowtrace.shared.persistence.repository.DirtyDayRepository
import dev.yidafu.flowtrace.shared.persistence.repository.DirtyDayRow
import dev.yidafu.flowtrace.shared.persistence.repository.SegmentScope
import dev.yidafu.flowtrace.shared.persistence.repository.TracePointCursor
import dev.yidafu.flowtrace.shared.persistence.repository.TracePointRepository
import dev.yidafu.flowtrace.shared.persistence.repository.TraceSegmentRepository
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit

/**
 * 迟到批的离线重算执行器（06 §3.6，08 §9）。
 *
 * 为什么必须有它：在线管道遇到「迟到批 / 重算锁」时**只写点、不分段**（点比段更不可补），
 * 那段轨迹的段就欠着。欠着的段如果不补，查询侧会看到「有原始点、无段」的空洞，
 * 里程/回放的分段口径全部失真。本类就是那个补账的作业。
 *
 * 调度语义：`@Scheduled(fixedDelay = dirty-scan-interval-sec)`，单实例内串行；
 * 多实例安全靠 [DirtyDayRepository.claimBatch] 的 `FOR UPDATE SKIP LOCKED`。
 *
 * 单日重算的三条口径（与在线链路必须一致，否则「重算后结果」会与「一直在线」不同）：
 * 1. **读窗口外扩 ±1h**：日界处的段要能衔接，06 §3.6 明确接受「边界误差 ≤1 段」；
 * 2. **只删/只写当日段**：`[dayStart, dayEnd)`，读到的前日尾部点只参与算法上下文，不落库；
 * 3. **产物一律 FINALIZED**：离线没有 OPEN 窗口概念，最后一段也必须封口。
 *
 * 幂等：重算对同一 `(user, day)` 重复执行的结果完全相同（`segmentId` 派生自 startTs +
 * 确定性算法），所以「算完没来得及 markDone 就崩溃」只会多算一次，不会写坏数据。
 */
@Component
class DirtyDayRecalculator(
    private val props: ProcessorProperties,
    private val dirtyDay: DirtyDayRepository,
    private val recalcGuard: RecalcGuard,
    private val pointRepository: TracePointRepository,
    private val segmentRepository: TraceSegmentRepository,
    private val builder: SegmentBuilder,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 扫描并处理一批待重算任务。
     *
     * 外层 `while(true)` 是「一直取到队列空」：一轮最多处理 [CLAIM_LIMIT] 条，
     * 积压时需要连续取，否则 60s 才推进 10 条，积压永远追不上。
     * 每轮都重新 claim（而不是复用上一轮的行）：claim 已把行置 RUNNING，
     * 失败/让路的行由 [DirtyDayRepository.markFailed] / [requeue] 改回 PENDING，下一轮才会再被挑到。
     *
     * **两种提前收工**（否则会自己转圈）：
     * - 整轮全部「让路」：锁被在线链路占着，立刻重新 claim 只会把同样的行再抢一遍，
     *   空转到锁释放为止——直接收工，等下一个调度周期；
     * - 出现失败：失败会 `markFailed`（attempts+1 → PENDING），同一轮再抢回来只会把
     *   三次重试在毫秒内烧完，把一次瞬时抖动打成 FAILED。收工，把重试间隔交给调度周期。
     */
    @Scheduled(fixedDelayString = $$"${flowtrace.processor.dirty-scan-interval-sec:60}", timeUnit = TimeUnit.SECONDS)
    fun scan() {
        var handled = 0
        while (true) {
            val claimed =
                try {
                    dirtyDay.claimBatch(CLAIM_LIMIT)
                } catch (ex: Exception) {
                    // DB 抖动：本轮放弃，下个周期再来，不把异常抛给调度线程（会把后续调度一起停掉）
                    log.error("processor 重算任务抢占失败，本轮跳过 cause={}", ex.message)
                    break
                }
            if (claimed.isEmpty()) break
            val outcomes = claimed.map { recalculate(it) }
            handled += claimed.size
            if (outcomes.all { it == Outcome.DEFERRED } || outcomes.any { it == Outcome.FAILED }) break
        }
        if (handled > 0) {
            log.info("processor 迟到重算轮次结束 tasks={}", handled)
        }
    }

    /** 单任务结局：决定本轮要不要继续抢下一批。 */
    private enum class Outcome { DONE, DEFERRED, FAILED }

    /** 处理单个 `(user, day)`：抢锁 → 重算 → 置 DONE / 失败重排。 */
    private fun recalculate(task: DirtyDayRow): Outcome {
        if (!recalcGuard.tryLock(task.userId)) {
            // 在线链路正在写同一用户：让路，不消耗 attempts（下一轮再试）
            dirtyDay.requeue(task.userId, task.day)
            log.debug("processor 重算让路（用户正被在线链路写入） userId={} day={}", task.userId, task.day)
            return Outcome.DEFERRED
        }
        val startNs = System.nanoTime()
        try {
            val written = recompute(task)
            dirtyDay.markDone(task.userId, task.day)
            log.info(
                "processor 迟到重算完成 userId={} day={} segments={} costMs={}",
                task.userId,
                task.day,
                written,
                (System.nanoTime() - startNs) / NANOS_PER_MILLI,
            )
            return Outcome.DONE
        } catch (ex: Exception) {
            // markFailed 自己可能也失败（DB 挂了），那是外层调度的事，这里只保证锁被释放
            val nextStatus = dirtyDay.markFailed(task.userId, task.day)
            log.error(
                "processor 迟到重算失败 userId={} day={} nextStatus={} costMs={} cause={}",
                task.userId,
                task.day,
                nextStatus,
                (System.nanoTime() - startNs) / NANOS_PER_MILLI,
                ex.message,
            )
            return Outcome.FAILED
        } finally {
            recalcGuard.unlock(task.userId)
        }
    }

    /**
     * 重算一个用户一天：读点 → 按 `(device, session)` 分组重建 → 单事务整体替换该日段。
     *
     * @return 写入的段数（日志与排查用）
     */
    fun recompute(task: DirtyDayRow): Int {
        val dayStart = startOfUtcDay(task.day)
        val dayEnd = dayStart + DAY_MS
        val points = readWindow(task.userId, dayStart - LOOKAROUND_MS, dayEnd + LOOKAROUND_MS)

        val groups: Map<SegmentScope, List<TraceSegmentDraft>> =
            points
                .groupBy { SegmentScope(userId = task.userId, deviceId = it.deviceId, sessionId = it.sessionId) }
                .mapValues { (scope, group) ->
                    builder
                        .build(
                            userId = scope.userId,
                            sessionId = scope.sessionId,
                            points = group,
                            epsilonM = props.dpStorageEpsilonM,
                            stayRadiusM = props.stayRadiusM,
                            stayMinMs = props.stayMinMs,
                            stayMergeGapMs = props.stayMergeGapMs,
                        ).filter { it.startTsMs >= dayStart && it.startTsMs < dayEnd }
                }.filterValues { it.isNotEmpty() }

        // 点全被清掉（或本来就没点）时也要执行一次：把该日残留的旧段删干净
        return segmentRepository.replaceDayRange(
            userId = task.userId,
            fromMsInclusive = dayStart,
            toMsExclusive = dayEnd,
            groups = groups,
            status = SegmentStatus.FINALIZED,
        )
    }

    /** 读 `[from, to)` 内该用户的全部点，按 `(ts, point_id)` keyset 翻页直到取空。 */
    private fun readWindow(
        userId: String,
        fromMs: Long,
        toMs: Long,
    ): List<TracePoint> {
        val all = ArrayList<TracePoint>()
        var cursor: TracePointCursor? = null
        while (true) {
            val page =
                pointRepository.findByUser(
                    userId = userId,
                    fromMsInclusive = fromMs,
                    toMsExclusive = toMs,
                    limit = READ_PAGE_SIZE,
                    after = cursor,
                )
            all += page
            if (page.size < READ_PAGE_SIZE) break
            val last = page.last()
            cursor = TracePointCursor(last.tsMs, last.pointId)
        }
        return all
    }

    private fun startOfUtcDay(day: LocalDate): Long = day.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()

    private companion object {
        /** 单轮抢占上限（08 §9 `claimBatch(10)`）。 */
        const val CLAIM_LIMIT = 10

        /** 日界外扩：保证日界处的段能衔接（06 §3.6 接受 ≤1 段的边界误差）。 */
        const val LOOKAROUND_MS = 3_600_000L

        const val DAY_MS = 86_400_000L
        const val READ_PAGE_SIZE = 5_000
        const val NANOS_PER_MILLI = 1_000_000L
    }
}
