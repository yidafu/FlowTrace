package dev.yidafu.flowtrace.upload.logging

import dev.yidafu.flowtrace.common.web.RequestPayloadRenderer
import dev.yidafu.flowtrace.v1.TraceBatchRequest
import org.springframework.stereotype.Component

/**
 * 上报请求体（`application/x-protobuf`）的日志摘要。
 *
 * 只在请求失败时被 [dev.yidafu.flowtrace.common.web.HttpRequestLoggingFilter] 调用，输出「哪一批、哪个设备、多少点、
 * 时间跨度、编码字节数」——排障够用，且**不含任何点位坐标**（07 §5 隐私约定，坐标为 PIE 敏感数据）。
 */
@Component
class TraceBatchPayloadRenderer : RequestPayloadRenderer {
    override fun supports(contentType: String?): Boolean = contentType?.contains("protobuf", ignoreCase = true) == true

    override fun render(
        contentType: String?,
        body: ByteArray,
    ): String =
        runCatching { TraceBatchRequest.parseFrom(body) }
            .map { describe(it) }
            .getOrElse { "<protobuf 无法解析：${it.javaClass.simpleName}: ${it.message}, ${body.size}B>" }

    private fun describe(batch: TraceBatchRequest): String {
        val header = batch.header
        return "TraceBatchRequest(batchId=${header.batchId}, deviceId=${header.deviceId}, userId=${header.userId}, " +
            "sessionId=${header.sessionId}, taskId=${header.taskId}, channel=${header.channel}, " +
            "schemaVersion=${header.schemaVersion}, sdk=${header.sdk.platform}/${header.sdk.sdkVersion}, " +
            "points=${batch.pointsCount}, tsRangeMs=${tsRangeMs(batch)}, baseTsMs=${header.base.tsMs}, " +
            "encodedBytes=${batch.serializedSize}) # 点位坐标属敏感位置数据，日志不输出"
    }

    /** 按 Delta 链（cur = prev + dTs）滚动累加出批内时间戳区间。 */
    private fun tsRangeMs(batch: TraceBatchRequest): String {
        if (batch.pointsCount == 0) return "-"
        var current = batch.header.base.tsMs
        var min = Long.MAX_VALUE
        var max = Long.MIN_VALUE
        batch.pointsList.forEach { point ->
            current += point.dTs
            min = minOf(min, current)
            max = maxOf(max, current)
        }
        return "$min..$max"
    }
}
