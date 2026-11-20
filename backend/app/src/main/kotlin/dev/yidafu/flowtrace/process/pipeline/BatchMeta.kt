package dev.yidafu.flowtrace.process.pipeline

import dev.yidafu.flowtrace.internal.v1.TraceMessage
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.UUID

/**
 * 一批的批头身份信息 + 时间范围（[TraceMessage.TraceBatchMessage] 的投影）。
 *
 * 单独成型而不是一路传 proto 消息：下游（清洗/分段/写库）只需要这几个字段，
 * 拿不到 points 列表也就无从误用 Delta 编码的中间值。
 *
 * 它是本批身份列的**唯一权威来源**：`DeviceState` 是跨批缓存，session/task 可能还停在上一批，
 * 因此清洗阶段就直接用批头的 userId/deviceId/sessionId/taskId 构造 [dev.yidafu.flowtrace.shared.model.TracePoint]，
 * 不存在「先按缓存身份建点、再整表 copy 覆盖」的第二道映射。
 *
 * [minTsMs] / [maxTsMs] 直接取点列首尾，**依赖 upload 已按 ts 升序展开**（`TraceUploadController`
 * 逐点滚动累加 Delta，天然有序）；批内乱序是清洗阶段才处理的事，不会在这里重排。
 */
data class BatchMeta(
    /** 批号（已解析）：`trace_point.batch_id` 列与台账状态更新用。 */
    val batchId: UUID,
    /**
     * 批号的**原样字符串**：`Ids.pointId` 必须按原样派生（解析再 `toString` 会改变大小写，
     * 同一物理点会得到两个 id，见 [dev.yidafu.flowtrace.shared.model.Ids.pointId]）。
     */
    val batchIdText: String,
    val userId: String,
    val deviceId: String,
    val sessionId: String,
    val taskId: String,
    /** 上报通道的原始数值（对应 `proto/trace.proto` 的 `Channel`：1 实时 / 2 补传）；下游按需转换，不在此提前转成模型枚举。 */
    val channel: Int,
    /** upload 侧接收时间（epoch ms）：用于算端到端时延，不参与水位判定（水位只看点 ts）。 */
    val receivedTsMs: Long,
    /** 批内最小点 ts（首个点）：迟到批判定用它与设备水位比较。 */
    val minTsMs: Long,
    /** 批内最大点 ts（末个点）：推进设备水位的值。 */
    val maxTsMs: Long,
) {
    /**
     * 本批覆盖的 **UTC 日**（含首尾，升序）：迟到批要按 `(user_id, day)` 登记 `dirty_day`。
     *
     * 必须用 UTC 切日——`trace_point` / `trace_segment` 的分区键与 `dirty_day.day` 列都按 UTC，
     * 用本地时区会在日界处错位一天（北京 00:30 的点属于前一个 UTC 日）。
     *
     * 上限 [MAX_COVERED_DAYS] 是防御：正常批受 upload 的点 ts 窗口约束（≤8 天），
     * 越界只可能来自脏数据，此时截断好过为一个坏批写上千条队列行。
     */
    fun coveredUtcDays(): List<LocalDate> {
        val start = Instant.ofEpochMilli(minTsMs).atZone(ZoneOffset.UTC).toLocalDate()
        val end = Instant.ofEpochMilli(maxTsMs).atZone(ZoneOffset.UTC).toLocalDate()
        if (end.isBefore(start)) return listOf(start)
        return generateSequence(start) { it.plusDays(1) }
            .takeWhile { !it.isAfter(end) }
            .take(MAX_COVERED_DAYS)
            .toList()
    }

    companion object {
        /** 单批最多覆盖的 UTC 天数（upload 允许的 ts 窗口上限 7 天 + 余量）。 */
        const val MAX_COVERED_DAYS: Int = 8

        /**
         * 前置条件：`pointsList` 非空且已按 ts 升序 —— 由 `TraceBatchListener` 的批级校验
         * （batchId 可解析为 UUID + pointsCount > 0）与 upload 的 Delta 展开顺序共同保证；
         * `batchId` 不可解析时监听器已投 DLQ，走不到这里。
         */
        fun of(message: TraceMessage.TraceBatchMessage): BatchMeta {
            val batchIdText = message.batchId
            return BatchMeta(
                batchId = UUID.fromString(batchIdText),
                batchIdText = batchIdText,
                userId = message.userId,
                deviceId = message.deviceId,
                sessionId = message.sessionId,
                taskId = message.taskId,
                channel = message.channel,
                receivedTsMs = message.receivedTsMs,
                minTsMs = message.pointsList.first().tsMs,
                maxTsMs = message.pointsList.last().tsMs,
            )
        }
    }
}
