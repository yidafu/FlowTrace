package dev.yidafu.flowtrace.processor.pipeline

import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.model.TracePoint

data class BatchMeta(
    val userId: String,
    val deviceId: String,
    val sessionId: String,
    val taskId: String,
    val channel: Int,
    val receivedTsMs: Long,
    val minTsMs: Long,
    val maxTsMs: Long,
) {
    companion object {
        fun of(message: TraceMessage.TraceBatchMessage): BatchMeta =
            BatchMeta(
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

/**
 * 清洗后的领域点 → 可写库行。
 *
 * 目前 [TracePoint] 与 `trace_point` 行是 1:1 映射，因此这里不是结构转换，而是
 * **用批头身份信息覆盖设备缓存里的可能旧值**：`DeviceState` 是跨批缓存，
 * session/task 可能还停留在上一批；批头才是本批的权威来源。
 *
 * 协议里 `session_id` / `task_id` 是 proto3 string，缺省为空串；库列 nullable，
 * 因此空串统一收敛为 null，避免写入空字符串后查询侧要写两套判空逻辑。
 */
fun TracePoint.toRow(meta: BatchMeta): TracePoint =
    copy(
        userId = meta.userId,
        deviceId = meta.deviceId,
        sessionId = meta.sessionId.ifEmpty { null },
        taskId = meta.taskId.ifEmpty { null },
    )
