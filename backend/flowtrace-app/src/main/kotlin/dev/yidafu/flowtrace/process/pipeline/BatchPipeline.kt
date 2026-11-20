package dev.yidafu.flowtrace.process.pipeline

import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.process.clean.DropReason
import dev.yidafu.flowtrace.process.clean.PointCleaner
import dev.yidafu.flowtrace.process.config.ProcessorProperties
import dev.yidafu.flowtrace.process.segment.DeviceStateStore
import dev.yidafu.flowtrace.process.segment.OnlineSegmenter
import dev.yidafu.flowtrace.process.store.BatchStatusSink
import dev.yidafu.flowtrace.process.store.RecalcGuard
import dev.yidafu.flowtrace.shared.cache.TombstoneStore
import dev.yidafu.flowtrace.shared.cache.WatermarkStore
import dev.yidafu.flowtrace.shared.model.DirtyDayReason
import dev.yidafu.flowtrace.shared.persistence.repository.DirtyDayRepository
import dev.yidafu.flowtrace.shared.persistence.repository.PointsWriter
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * 单批处理结果：供日志断点与指标（08 §12「每批一行 info」）消费。
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
private fun Map<DropReason, Int>.toCountText(): String =
    if (isEmpty()) "-" else entries.joinToString(",") { "${it.key.name.lowercase()}=${it.value}" }

/**
 * 批处理主链路：墓碑 → 水位 → 清洗 → 落点 → 分段 → 批状态。
 *
 * 日志断点约定（默认 info 只出「批级摘要」，细节在 debug）：
 * - INFO：墓碑命中、迟到批跳过分段、重算锁占用跳过分段——这三种都是「数据被有意丢弃/跳过」，
 *   事后排查「点没进库」时第一眼要看的就是它们；
 * - DEBUG：水位读数、清洗保留/丢弃计数、写库行数、分段写入结果、markProcessed。
 *
 * [process] 返回 [BatchResult] 而不是 Unit：监听器据此打一行批摘要（08 §12），
 * 避免同一批在管道里散落多行 info。
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
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 处理一批：墓碑 → 水位 → 清洗 → 落点 → 分段 → 批状态。
     *
     * 三条「提前返回」的岔路都不抛异常（异常会被容器当可重试而卡分区），但**台账状态各不相同**：
     * - 墓碑命中（用户已撤销/删除）：整批丢，连点都不写 → `FAILED(TOMBSTONE)`；
     * - 迟到批：点照常落库，**登记 `dirty_day`** 后交离线重算补段 → `PROCESSED`；
     * - 重算锁占用：同上（点落库 + 入 dirty_day，分段让给重算）→ `PROCESSED`。
     *
     * 墓碑/迟到都不 mark 的旧口径会让台账永远停在 ACCEPTED，对账时无法区分
     * 「还没处理」和「处理完了但被有意跳过」，因此按 06 §3.2 第 2/6 步改正。
     *
     * 其余异常一律向上抛：DB / Redis 抖动是可重试的，容器按退避原地重试、该分区不前进；
     * 毒丸消息由 `TraceBatchListener` 按 `batchTimeoutMs` 判定后转 DLQ。
     *
     * 幂等口径：`pointId`（batchId+idx）与 `segmentId`（userId+sessionId+startTs）都是确定性派生，
     * 所以「已 ack 未提交」导致的重放是安全的；`dirty_day` 入队也带 ON CONFLICT DO NOTHING。
     */
    fun process(message: TraceMessage.TraceBatchMessage): BatchResult {
        val meta = BatchMeta.of(message)
        val batchId = meta.batchId

        // 1. 墓碑：Redis 不可用时**抛出**（不 ack、退避重试）——隐私优先，
        //    绝不能因为 Redis 抖动把已删用户的数据写回去（08 §7「宁严不宽」）
        if (tombstone.has(userId = message.userId)) {
            log.info(
                "processor 墓碑命中，整批丢弃 batchId={} deviceId={} userId={} points={}",
                message.batchId,
                message.deviceId,
                message.userId,
                message.pointsCount,
            )
            batchStatus.markFailed(batchId, TOMBSTONE)
            return BatchResult(
                pointsIn = message.pointsCount,
                kept = 0,
                dropped = emptyMap(),
                tombstoned = true,
            )
        }

        // 2. 水位：读旧值判迟到 → 推进新值（顺序不可交换，反了每批都判自己迟到）
        var wm: Long? = null
        var watermarkReadable = true
        try {
            wm = watermark.maxSeenTs(deviceId = message.deviceId)
        } catch (ex: Exception) {
            // 降级：宁可不判迟到，也不能把正常批误判为迟到而跳过分段（段会永久缺失）
            watermarkReadable = false
            log.warn(
                "processor 水位读取失败，按非迟到处理且不推进水位 batchId={} deviceId={} cause={}",
                message.batchId,
                message.deviceId,
                ex.message,
            )
        }
        val late = watermarkReadable && wm != null && meta.minTsMs < wm - props.watermarkDriftMs
        if (watermarkReadable) {
            try {
                watermark.writeIfGreater(meta.deviceId, meta.maxTsMs)
            } catch (ex: Exception) {
                log.warn(
                    "processor 水位推进失败（本批继续，下一批可能重复判迟到） batchId={} deviceId={} cause={}",
                    message.batchId,
                    message.deviceId,
                    ex.message,
                )
            }
        }
        log.debug(
            "processor 水位检查 batchId={} deviceId={} prevMaxSeenTs={} batchMinTsMs={} batchMaxTsMs={} late={} degraded={}",
            message.batchId,
            meta.deviceId,
            wm,
            meta.minTsMs,
            meta.maxTsMs,
            late,
            !watermarkReadable,
        )

        // 3. 清洗（跨批 lastKept 状态；被丢的点不入库）
        //    设备状态每批只取一次：清洗与分段共用同一份内存状态，避免两处 getOrLoad 之间被 LRU 淘汰
        //    （淘汰会先把 OPEN 窗口落库再丢状态，此时清洗刚写回的 lastKept 就白丢了）
        val state = deviceState.getOrLoad(deviceId = meta.deviceId)
        val cleaned =
            cleaner.clean(
                points = message.pointsList,
                meta = meta,
                state = state,
                nowMs = System.currentTimeMillis(),
            )
        log.debug(
            "processor 清洗完成 batchId={} deviceId={} in={} kept={} dropped={}",
            message.batchId,
            meta.deviceId,
            message.pointsCount,
            cleaned.points.size,
            cleaned.dropped.toCountText(),
        )

        // 4. 幂等写点（ON CONFLICT DO NOTHING，返回尝试行数）
        //    清洗产物已按批头身份构造，可直接当库行写，不需要再整表 copy 覆盖一遍
        if (cleaned.isNotEmpty()) {
            val rows = pointsWriter.batchInsertIgnore(cleaned.points)
            log.debug(
                "processor 轨迹点写入完成 batchId={} deviceId={} rows={} kept={}",
                message.batchId,
                meta.deviceId,
                rows,
                cleaned.points.size,
            )
        }

        // 5. 迟到 / 重算锁：点已落库，分段交给离线重算——必须先登记 dirty_day，否则段永久缺失
        val recalcLocked = recalcGuard.isLocked(meta.userId)
        if (late || recalcLocked) {
            val days = meta.coveredUtcDays()
            dirtyDay.enqueue(meta.userId, days, DirtyDayReason.LATE_DATA)
            if (late) {
                log.info(
                    "processor 迟到批跳过分段并登记重算 batchId={} deviceId={} days={} batchMinTsMs={} prevMaxSeenTs={} driftMs={}",
                    message.batchId,
                    meta.deviceId,
                    days.size,
                    meta.minTsMs,
                    wm,
                    props.watermarkDriftMs,
                )
            } else {
                log.info(
                    "processor 重算锁占用，跳过分段并登记重算 batchId={} deviceId={} userId={} days={}",
                    message.batchId,
                    meta.deviceId,
                    meta.userId,
                    days.size,
                )
            }
            // 点已落库、补偿已登记 → 该批在本服务的职责已结束，台账置 PROCESSED
            batchStatus.markProcessed(batchId)
            return BatchResult(
                pointsIn = message.pointsCount,
                kept = cleaned.points.size,
                dropped = cleaned.dropped,
                late = late,
                recalcSkipped = recalcLocked && !late,
                watermarkDegraded = !watermarkReadable,
                queuedDays = days.size,
            )
        }

        // 6. 在线分段（每批 flush，尾段 OPEN；切分时闭合并落 FINALIZED）
        val segments = segmenter.onBatch(meta, cleaned.points, wm, state)

        // 7. 批状态（异步缓冲，失败只记日志，不影响 ack）
        batchStatus.markProcessed(batchId)
        log.debug(
            "processor 批状态置 PROCESSED batchId={} deviceId={} segments={}",
            message.batchId,
            meta.deviceId,
            segments,
        )

        return BatchResult(
            pointsIn = message.pointsCount,
            kept = cleaned.points.size,
            dropped = cleaned.dropped,
            watermarkDegraded = !watermarkReadable,
            segments = segments,
        )
    }

    private companion object {
        /** 墓碑丢弃的 `trace_batch.error_code`（06 §3.2 第 2 步）。 */
        const val TOMBSTONE = "TOMBSTONE"
    }
}
