package dev.yidafu.flowtrace.process.pipeline

import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.process.clean.DropReason
import dev.yidafu.flowtrace.process.clean.PointCleaner
import dev.yidafu.flowtrace.process.config.ProcessorProperties
import dev.yidafu.flowtrace.process.metrics.ProcessorMetrics
import dev.yidafu.flowtrace.process.segment.DeviceState
import dev.yidafu.flowtrace.process.segment.DeviceStateStore
import dev.yidafu.flowtrace.process.segment.OnlineSegmenter
import dev.yidafu.flowtrace.process.store.BatchStatusSink
import dev.yidafu.flowtrace.process.store.RecalcGuard
import dev.yidafu.flowtrace.shared.cache.TombstoneStore
import dev.yidafu.flowtrace.shared.cache.WatermarkStore
import dev.yidafu.flowtrace.shared.model.DirtyDayReason
import dev.yidafu.flowtrace.shared.persistence.repository.DirtyDayRepository
import dev.yidafu.flowtrace.shared.persistence.repository.PointsWriter
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * 单批处理结果：供日志断点与指标（08 §12「每 cycle 一行 info」）消费。
 *
 * 只装计数与布尔，**不含坐标**（07 §5「日志不出坐标」）。
 */
data class BatchResult(
    val pointsIn: Int,
    val kept: Int,
    val dropped: Map<DropReason, Int>,
    val late: Boolean = false,
    val tombstoned: Boolean = false,
    val recalcSkipped: Boolean = false,
    /** 水位不可用（Redis 抖动）：本批按「非迟到 + 不推进水位」处理（08 §7）。 */
    val watermarkDegraded: Boolean = false,
    /** 因迟到/重算让路而入队的 UTC 天数（0 表示未入队）。 */
    val queuedDays: Int = 0,
    val segments: Int = 0,
) {
    /** 丢弃原因压成一行：`zero=3,duplicate=1`；无丢弃时 `-`。 */
    fun droppedText(): String = dropped.toCountText()
}

/** 丢弃原因计数压成一行：`zero=3,duplicate=1`；无丢弃时 `-`。 */
internal fun Map<DropReason, Int>.toCountText(): String =
    if (isEmpty()) "-" else entries.joinToString(",") { "${it.key.name.lowercase()}=${it.value}" }

/**
 * 批处理主链路：墓碑 → 水位 → 清洗 → 落点 → 分段 → 批状态。
 *
 * **合批口径（08 §4「poll 攒 N 批合并写」，2026-11-21）**：一个 poll cycle（上限
 * `poll-records`）内的全部批共用——
 * - 一次 MGET 查墓碑、一次 MGET 读水位、一次多键 EVAL 推水位、一次 MGET 查重算锁；
 * - 一个 DB 事务装下全部批的点位 INSERT + 逐批分段 flush（或迟到补偿登记），
 *   全 cycle 只剩 1 次提交（1 次 fsync）；
 * - 清洗 / 分段仍逐批执行（同设备顺序 = poll 的 offset 顺序，天然保序）。
 *
 * 日志断点约定（默认 info 只出「cycle 级摘要」，细节在 debug）：
 * - INFO：墓碑命中、迟到批跳过分段、重算锁占用跳过分段、cycle 摘要——前三种都是
 *   「数据被有意丢弃/跳过」，事后排查「点没进库」时第一眼要看的就是它们；
 * - DEBUG：水位读数、清洗保留/丢弃计数、写库行数、markProcessed。
 *
 * [processAll] 返回逐批 [BatchResult]（顺序与入参一致）：监听器据此记指标与 cycle 摘要，
 * 管道本身保持纯逻辑（不依赖 Micrometer），BatchResult 就是它的计数契约。
 */
@Component
class BatchPipeline(
    private val tombstone: TombstoneStore,
    private val watermark: WatermarkStore,
    private val props: ProcessorProperties,
    private val deviceState: DeviceStateStore,
    private val cleaner: PointCleaner,
    private val pointsWriter: PointsWriter,
    private val recalcGuard: RecalcGuard,
    private val segmenter: OnlineSegmenter,
    private val batchStatus: BatchStatusSink,
    private val dirtyDay: DirtyDayRepository,
    private val metrics: ProcessorMetrics,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 分段计时包装：异常也要计入耗时，所以放 `finally`。
     *
     * 为什么用墙钟（nanoTime）而不是 CPU 时间：这条链路的瓶颈是 IO 等待
     * （DB 提交、Redis 往返），CPU 采样看不见它（docs/design/09-performance.md §9）。
     */
    private inline fun <T> stage(
        name: String,
        block: () -> T,
    ): T {
        val start = System.nanoTime()
        try {
            return block()
        } finally {
            metrics.stage(name, System.nanoTime() - start)
        }
    }

    /** 单批入口（测试与兼容路径）：等价于 cycle = 1 的 [processAll]。 */
    fun process(message: TraceMessage.TraceBatchMessage): BatchResult = processAll(listOf(message)).single()

    /**
     * 处理一个 poll cycle 的全部批（08 §4）。每批的分支语义与单批时代一致：
     *
     * 三条「提前返回」的岔路都不抛异常（异常会被容器当可重试而卡分区），但**台账状态各不相同**：
     * - 墓碑命中（用户已撤销/删除）：整批丢，连点都不写 → `FAILED(TOMBSTONE)`；
     * - 迟到批：点照常落库，**登记 `dirty_day`** 后交离线重算补段 → `PROCESSED`；
     * - 重算锁占用：同上（点落库 + 入 dirty_day，分段让给重算）→ `PROCESSED`。
     *
     * 墓碑/迟到都不 mark 的旧口径会让台账永远停在 ACCEPTED，对账时无法区分
     * 「还没处理」和「处理完了但被有意跳过」，因此按 06 §3.2 第 2/6 步改正。
     *
     * 其余异常一律向上抛：DB / Redis 抖动是可重试的，容器按退避原地重试、**整个 cycle
     * 重放**（重放放大上限 = `poll-records`）；该分区不前进。毒丸消息由 [TraceBatchListener]
     * 按 cycle 墙钟超过 `batchTimeoutMs` 判定后转 DLQ。
     *
     * 幂等口径：`pointId`（batchId+idx）与 `segmentId`（userId+sessionId+startTs）都是确定性派生，
     * 所以「已 ack 未提交」导致的重放是安全的；`dirty_day` 入队也带 ON CONFLICT DO NOTHING。
     */
    fun processAll(messages: List<TraceMessage.TraceBatchMessage>): List<BatchResult> {
        if (messages.isEmpty()) return emptyList()
        return stage("total") { processAllInner(messages) }
    }

    /** cycle 内一个待处理批的槽位：[index] 指回 messages/metas，供清洗结果与结果装配对位。 */
    private data class CycleBatch(
        val index: Int,
        val meta: BatchMeta,
    )

    private fun processAllInner(messages: List<TraceMessage.TraceBatchMessage>): List<BatchResult> {
        val metas = messages.map { BatchMeta.of(it) }

        // 1. 墓碑：一次 MGET 覆盖 cycle 内全部 userId。Redis 不可用时**抛出**（不 ack、退避重试）——
        //    隐私优先，绝不能因为 Redis 抖动把已删用户的数据写回去（08 §7「宁严不宽」）
        val tombstonedUsers = stage("tombstone") { tombstone.filterTombstoned(metas.map { it.userId }) }
        val aliveBatches =
            metas
                .mapIndexed { i, meta ->
                    if (meta.userId in tombstonedUsers) {
                        log.info(
                            "processor 墓碑命中，整批丢弃 batchId={} deviceId={} userId={} points={}",
                            meta.batchIdText,
                            meta.deviceId,
                            meta.userId,
                            messages[i].pointsCount,
                        )
                        batchStatus.markFailed(meta.batchId, TOMBSTONE)
                        null
                    } else {
                        CycleBatch(i, meta)
                    }
                }.filterNotNull()
        if (aliveBatches.isEmpty()) {
            return messages.map { message ->
                BatchResult(pointsIn = message.pointsCount, kept = 0, dropped = emptyMap(), tombstoned = true)
            }
        }

        // 2. 水位：读旧值判迟到 → 推进新值（顺序不可交换，反了每批都判自己迟到）。
        //    合批：一次 MGET 读全部设备；同设备多批共用同一读数（cycle 内的推进发生在读取之后，
        //    与「逐批先读后写」的终态一致）
        var watermarkReadable = true
        val wmByDevice: Map<String, Long> =
            try {
                stage("watermark") { watermark.maxSeenTsBatch(aliveBatches.map { it.meta.deviceId }) }
            } catch (ex: Exception) {
                // 降级：宁可不判迟到，也不能把正常批误判为迟到而跳过分段（段会永久缺失）
                watermarkReadable = false
                log.warn(
                    "processor 水位读取失败，本 cycle 全部按非迟到处理且不推进水位 batches={} cause={}",
                    aliveBatches.size,
                    ex.message,
                )
                emptyMap()
            }
        val aliveLate =
            aliveBatches.map { batch ->
                val wm = wmByDevice[batch.meta.deviceId]
                wm != null && batch.meta.minTsMs < wm - props.watermarkDriftMs
            }
        if (watermarkReadable) {
            // 同设备多批只传一次、取最大批尾 ts：writeIfGreater 单调，合并写与逐批写终态一致
            val wmWrites = LinkedHashMap<String, Long>()
            aliveBatches.forEach { batch ->
                wmWrites.merge(batch.meta.deviceId, batch.meta.maxTsMs) { prev, next -> maxOf(prev, next) }
            }
            try {
                stage("watermark") { watermark.writeIfGreaterBatch(wmWrites) }
            } catch (ex: Exception) {
                log.warn(
                    "processor 水位推进失败（本 cycle 继续，下一 cycle 可能重复判迟到） devices={} cause={}",
                    wmWrites.size,
                    ex.message,
                )
            }
        }
        log.debug(
            "processor 水位读数 batches={} devices={} degraded={}",
            aliveBatches.size,
            wmByDevice.size,
            !watermarkReadable,
        )

        // 3. 清洗（跨批 lastKept 状态；被丢的点不入库）。
        //    clean_state / clean_points 子阶段嵌套计时（与 db_txn / db_points 同款），合批后按
        //    cycle 计一次：区分「状态回源（LRU miss = 3 条 SQL）」与「逐点清洗计算」的占比（09 §4 第 6 项）。
        val states = arrayOfNulls<DeviceState>(messages.size)
        val cleaned = arrayOfNulls<PointCleaner.Result>(messages.size)
        stage("clean") {
            // 设备状态每批只取一次：清洗与分段共用同一份内存状态，避免两处 getOrLoad 之间被 LRU 淘汰
            // （淘汰会先把 OPEN 窗口落库再丢状态，此时清洗刚写回的 lastKept 就白丢了）
            stage("clean_state") {
                aliveBatches.forEach { batch -> states[batch.index] = deviceState.getOrLoad(batch.meta.deviceId) }
            }
            stage("clean_points") {
                aliveBatches.forEach { batch ->
                    cleaned[batch.index] =
                        cleaner.clean(
                            points = messages[batch.index].pointsList,
                            meta = batch.meta,
                            state = states[batch.index]!!,
                            nowMs = System.currentTimeMillis(),
                        )
                }
            }
        }
        aliveBatches.forEach { batch ->
            val result = cleaned[batch.index]!!
            log.debug(
                "processor 清洗完成 batchId={} deviceId={} in={} kept={} dropped={}",
                batch.meta.batchIdText,
                batch.meta.deviceId,
                messages[batch.index].pointsCount,
                result.points.size,
                result.dropped.toCountText(),
            )
        }

        // 4. 重算锁：Redis 读（一次 MGET），**放在事务外**——不能在 DB 事务里夹一次 Redis 往返
        //    （会把事务拖长）。查询失败由 RecalcGuard 按「全部已加锁」处理（宁严不宽）
        val lockedUsers = stage("recalc_guard") { recalcGuard.filterLocked(aliveBatches.map { it.meta.userId }) }
        val aliveSkipSegments = aliveLate.mapIndexed { j, late -> late || aliveBatches[j].meta.userId in lockedUsers }

        // 5. 点 + 段（或补偿登记）在**同一个事务**里提交——合批后连 cycle 内全部批也共用这一个事务。
        //
        // 为什么合并：写段一批 30 点常触发 2–3 次 flush（切分 + 批末），事务合并前每次 flush 各自
        // 一个事务；叠加台账 UPDATE 后实测 ≈10 次提交/批（594 批/s → 3.4k commit/s，PG 侧
        // `LWLock: WALWrite` 占会话 13.5%）。合批进一步把「每批 1 次提交」压成「每 cycle 1 次」，
        // 解码 / 墓碑 / 水位 / 重算这些 Redis 往返也随之按 cycle 摊薄（08 §4）。
        // 仓库层的 `transaction {}` 会加入本事务，不再各自提交。
        //
        // 重试语义：任一批抛可重试异常，整个 cycle 不 ack 原地重放（放大上限 = poll-records）；
        // 幂等由 pointId / segmentId 的确定性派生 + ON CONFLICT DO NOTHING 吸收。
        // 语义不变：点与段要么一起提交、要么一起回滚。
        val segments = IntArray(messages.size)
        stage("db_txn") {
            transaction {
                val rows = aliveBatches.flatMap { cleaned[it.index]!!.points }
                if (rows.isNotEmpty()) {
                    stage("db_points") {
                        pointsWriter.batchInsertIgnore(rows)
                    }
                    log.debug(
                        "processor 轨迹点写入完成 batches={} rows={}",
                        aliveBatches.size,
                        rows.size,
                    )
                }
                val skippedBatches = aliveBatches.filterIndexed { j, _ -> aliveSkipSegments[j] }
                val normalBatches = aliveBatches.filterIndexed { j, _ -> !aliveSkipSegments[j] }
                if (skippedBatches.isNotEmpty()) {
                    // 迟到 / 重算锁：分段交给离线重算，但补偿登记必须先落库，否则段永久缺失
                    stage("db_dirty") {
                        skippedBatches.forEach { batch ->
                            dirtyDay.enqueue(batch.meta.userId, batch.meta.coveredUtcDays(), DirtyDayReason.LATE_DATA)
                        }
                    }
                }
                if (normalBatches.isNotEmpty()) {
                    // 6. 在线分段（每批 flush，尾段 OPEN；切分时闭合并落 FINALIZED）。
                    //    按 poll 的原始顺序逐批 flush：同设备多批天然保序（分区有序）
                    stage("db_segment") {
                        normalBatches.forEach { batch ->
                            segments[batch.index] =
                                segmenter.onBatch(
                                    batch.meta,
                                    cleaned[batch.index]!!.points,
                                    wmByDevice[batch.meta.deviceId],
                                    states[batch.index]!!,
                                )
                        }
                    }
                }
            }
        }

        aliveBatches.forEachIndexed { j, batch ->
            if (!aliveSkipSegments[j]) return@forEachIndexed
            if (aliveLate[j]) {
                log.info(
                    "processor 迟到批跳过分段并登记重算 batchId={} deviceId={} userId={} days={} batchMinTsMs={} prevMaxSeenTs={} driftMs={}",
                    batch.meta.batchIdText,
                    batch.meta.deviceId,
                    batch.meta.userId,
                    batch.meta.coveredUtcDays().size,
                    batch.meta.minTsMs,
                    wmByDevice[batch.meta.deviceId],
                    props.watermarkDriftMs,
                )
            } else {
                log.info(
                    "processor 重算锁占用，跳过分段并登记重算 batchId={} deviceId={} userId={} days={}",
                    batch.meta.batchIdText,
                    batch.meta.deviceId,
                    batch.meta.userId,
                    batch.meta.coveredUtcDays().size,
                )
            }
        }

        // 7. 批状态（异步缓冲，失败只记日志，不影响 ack；刻意留在事务外，不反压主链路）
        stage("status_enqueue") {
            aliveBatches.forEach { batch -> batchStatus.markProcessed(batch.meta.batchId) }
        }
        log.debug("processor 批状态置 PROCESSED batches={}", aliveBatches.size)

        return messages.mapIndexed { i, message ->
            val j = aliveBatches.indexOfFirst { it.index == i }
            if (j < 0) {
                BatchResult(pointsIn = message.pointsCount, kept = 0, dropped = emptyMap(), tombstoned = true)
            } else {
                val result = cleaned[i]!!
                BatchResult(
                    pointsIn = message.pointsCount,
                    kept = result.points.size,
                    dropped = result.dropped,
                    late = aliveLate[j],
                    recalcSkipped = (metas[i].userId in lockedUsers) && !aliveLate[j],
                    watermarkDegraded = !watermarkReadable,
                    queuedDays = if (aliveSkipSegments[j]) metas[i].coveredUtcDays().size else 0,
                    segments = segments[i],
                )
            }
        }
    }

    private companion object {
        /** 墓碑丢弃的 `trace_batch.error_code`（06 §3.2 第 2 步）。 */
        const val TOMBSTONE = "TOMBSTONE"
    }
}
