package dev.yidafu.flowtrace.processor.pipeline

import dev.yidafu.flowtrace.common.cache.TombstoneStore
import dev.yidafu.flowtrace.common.cache.WatermarkStore
import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.processor.clean.DropReason
import dev.yidafu.flowtrace.processor.clean.PointCleaner
import dev.yidafu.flowtrace.processor.config.ProcessorProperties
import dev.yidafu.flowtrace.processor.segment.DeviceStateStore
import dev.yidafu.flowtrace.processor.segment.OnlineSegmenter
import dev.yidafu.flowtrace.processor.store.RecalcGuard
import dev.yidafu.flowtrace.storage.postgres.repository.PointsWriter
import dev.yidafu.flowtrace.storage.postgres.repository.TraceBatchRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.UUID

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
    private val batchStatus: TraceBatchRepository,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun process(message: TraceMessage.TraceBatchMessage): BatchResult {
        val batchId = UUID.fromString(message.batchId)
        val meta = BatchMeta.of(message)

        // 墓碑. 检查用户是否被删除了
        if (tombstone.has(userId = message.userId)) {
            // 命中只丢批；不能再 mark——那会刷新墓碑 TTL，而且这里根本不是写墓碑的地方
            log.info(
                "processor 墓碑命中，整批丢弃 batchId={} deviceId={} userId={} points={}",
                message.batchId,
                message.deviceId,
                message.userId,
                message.pointsCount,
            )
            return BatchResult(
                pointsIn = message.pointsCount,
                kept = 0,
                dropped = emptyMap(),
                tombstoned = true,
            )
        }

        // 水位检查. 检查是否在上传时间窗口里
        val wm = watermark.maxSeenTs(deviceId = message.deviceId)
        val late = wm != null && meta.minTsMs < wm - props.watermarkDriftMs
        watermark.writeIfGreater(meta.deviceId, meta.maxTsMs)
        log.debug(
            "processor 水位检查 batchId={} deviceId={} prevMaxSeenTs={} batchMinTsMs={} batchMaxTsMs={} late={}",
            message.batchId,
            meta.deviceId,
            wm,
            meta.minTsMs,
            meta.maxTsMs,
            late,
        )

        // 清理
        val state = deviceState.getOrLoad(deviceId = meta.deviceId)
        val cleaned =
            cleaner.clean(
                points = message.pointsList,
                state = state,
                nowMs = System.currentTimeMillis(),
                batchId = message.batchId,
            )
        log.debug(
            "processor 清洗完成 batchId={} deviceId={} in={} kept={} dropped={}",
            message.batchId,
            meta.deviceId,
            message.pointsCount,
            cleaned.points.size,
            cleaned.dropped.toCountText(),
        )

        if (cleaned.isNotEmpty()) {
            val rows =
                pointsWriter.batchInsertIgnore(
                    cleaned.points.map { it.toRow(meta) },
                )
            log.debug(
                "processor 轨迹点写入完成 batchId={} deviceId={} rows={} kept={}",
                message.batchId,
                meta.deviceId,
                rows,
                cleaned.points.size,
            )
        }

        val recalcLocked = recalcGuard.isLocked(meta.userId)
        if (late) {
            log.info(
                "processor 迟到批跳过分段（点已落库，等重算补齐） batchId={} deviceId={} batchMinTsMs={} prevMaxSeenTs={} driftMs={}",
                message.batchId,
                meta.deviceId,
                meta.minTsMs,
                wm,
                props.watermarkDriftMs,
            )
        } else if (recalcLocked) {
            log.info(
                "processor 重算锁占用，跳过分段 batchId={} deviceId={} userId={}",
                message.batchId,
                meta.deviceId,
                meta.userId,
            )
        }
        if (late || recalcLocked) {
            return BatchResult(
                pointsIn = message.pointsCount,
                kept = cleaned.points.size,
                dropped = cleaned.dropped,
                late = late,
                recalcSkipped = recalcLocked && !late,
            )
        }

        // 在线分度
        val segments = segmenter.onBatch(meta, cleaned.points, wm)
        // 更新批状态
        batchStatus.markProcessed(batchId)
        log.debug(
            "processor 批状态已置 PROCESSED batchId={} deviceId={} segments={}",
            message.batchId,
            meta.deviceId,
            segments,
        )

        return BatchResult(
            pointsIn = message.pointsCount,
            kept = cleaned.points.size,
            dropped = cleaned.dropped,
            segments = segments,
        )
    }
}
