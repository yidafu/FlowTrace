package dev.yidafu.flowtrace.processor.segment

import dev.yidafu.flowtrace.model.GeoMath
import dev.yidafu.flowtrace.model.MoveMode
import dev.yidafu.flowtrace.model.SegmentStatus
import dev.yidafu.flowtrace.model.TracePoint
import dev.yidafu.flowtrace.processor.config.ProcessorProperties
import dev.yidafu.flowtrace.processor.pipeline.BatchMeta
import dev.yidafu.flowtrace.storage.postgres.repository.SegmentScope
import dev.yidafu.flowtrace.storage.postgres.repository.TraceSegmentRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * 在线分段：逐点判断窗口是否切分（时间间隔 / 跳变 / 运动模式变化），
 * 未闭合窗口每批 flush 一次（OPEN），切分时闭合并落 FINALIZED。
 *
 * 返回值是本次写入/重写的**段数**，供 [BatchPipeline] 汇总成一行批日志（08 §12 `segments`）。
 * 日志只打 ID/计数/时间戳与切分原因，**不打坐标与距离**（07 §5）。
 */
@Component
class OnlineSegmenter(
    private val deviceState: DeviceStateStore,
    private val props: ProcessorProperties,
    private val writer: TraceSegmentRepository,
    private val builder: SegmentBuilder,
    private val classifier: MoveModeClassifier,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    init {
        // 装配淘汰/停机回调（08 §8.1）：设备状态被 LRU 淘汰或进程停机时，把当前 OPEN 窗口按 OPEN 状态
        // 落库，下次 `getOrLoad` 重建窗口时续写（窗口被截断则由 `openTruncated` 先封口旧段）。
        // 不装配时 `DeviceStateStore` 只会打 error——内存窗口一丢，这段轨迹在库里就永远缺一段。
        // 注意 close=false：淘汰不是闭合语义，落 OPEN 才能续写，不能写成 FINALIZED。
        deviceState.flusher = { state -> flush(state, close = false) }
    }

    fun onBatch(
        meta: BatchMeta,
        points: List<TracePoint>,
        watermark: Long?,
    ): Int {
        val state = deviceState.getOrLoad(meta.deviceId)
        state.userId = meta.userId
        state.sessionId = meta.sessionId
        state.taskId = meta.taskId
        var written = 0
        for (point in points) {
            val open = state.open
            if (open != null) {
                val last = open.last
                if (point.tsMs <= last.tsMs) {
                    log.debug(
                        "processor 乱序点跳过分段 deviceId={} pointTsMs={} windowLastTsMs={}",
                        meta.deviceId,
                        point.tsMs,
                        last.tsMs,
                    )
                    continue
                }

                val gapMs = point.tsMs - last.tsMs
                val jumpM = GeoMath.haversineM(last.latQ, last.lngQ, point.latQ, point.lngQ)
                val newMode = classifier.confirmedMode(point, open.moveMode)

                val split =
                    gapMs > props.splitGapMs ||
                        jumpM > props.splitJumpM ||
                        newMode != open.moveMode

                if (split) {
                    val reason =
                        buildList {
                            if (gapMs > props.splitGapMs) add("gap")
                            if (jumpM > props.splitJumpM) add("jump")
                            if (newMode != open.moveMode) add("mode")
                        }.joinToString("+")
                    log.debug(
                        "processor 分段切分 deviceId={} reason={} gapMs={} mode={}=>{}",
                        meta.deviceId,
                        reason,
                        gapMs,
                        open.moveMode,
                        newMode,
                    )
                    // 切分点前后两条段都写库：flush(close=true) 会先把当前窗口闭合为 FINALIZED
                    written += flush(state, close = true, watermark = watermark)
                }
            }

            // 新窗口必须带着首点构造：[OpenWindow] 约定窗口至少一个点，先建空窗口再 append 会直接抛
            if (state.open == null) {
                state.open =
                    OpenWindow(
                        startTsMs = point.tsMs,
                        points = mutableListOf(point),
                        moveMode = classifier.confirmedMode(point, MoveMode.UNKNOWN),
                    )
            } else {
                state.open!!.points += point
            }
            if (state.open!!.points.size >= props.openSegMaxPoints) {
                log.debug(
                    "processor 分段窗口达到上限，强制闭合 deviceId={} maxPoints={}",
                    meta.deviceId,
                    props.openSegMaxPoints,
                )
                written += flush(state, close = true, watermark = watermark)
            }
        }

        written += flush(state, close = false, watermark = watermark)
        return written
    }

    /** @return 本次 replaceTail 写入/重写的段数（无 OPEN 窗口或无 userId 时为 0）。 */
    fun flush(
        state: DeviceState,
        close: Boolean = false,
        watermark: Long? = null,
    ): Int {
        val open = state.open ?: return 0
        val userId = state.userId ?: return 0
        val scope =
            SegmentScope(
                userId = userId,
                sessionId = state.sessionId,
                deviceId = state.deviceId,
            )
        if (state.openTruncated) {
            log.debug(
                "processor 截断窗口恢复：先封口旧 OPEN 段 deviceId={} beforeTsMs={}",
                state.deviceId,
                open.startTsMs,
            )
            writer.finalizeOpenBefore(scope, open.startTsMs)
            state.openTruncated = false
        }

        val drafts =
            builder.build(
                userId = userId,
                sessionId = state.sessionId,
                points = open.points,
                epsilonM = props.dpStorageEpsilonM,
                stayRadiusM = props.stayRadiusM,
                stayMinMs = props.stayMinMs,
                stayMergeGapMs = props.stayMergeGapMs,
            )

        val finalized = close || (watermark != null && open.last.tsMs < watermark - props.watermarkDriftMs)

        writer.replaceTail(scope, open.startTsMs, drafts = drafts, status = if (finalized) SegmentStatus.FINALIZED else SegmentStatus.OPEN)
        log.debug(
            "processor 分段写入 deviceId={} userId={} windowStartTsMs={} windowPoints={} segments={} status={} close={}",
            state.deviceId,
            userId,
            open.startTsMs,
            open.points.size,
            drafts.size,
            if (finalized) SegmentStatus.FINALIZED else SegmentStatus.OPEN,
            close,
        )
        if (close) {
            state.open = null
        }
        return drafts.size
    }
}
