package dev.yidafu.flowtrace.shared.model

/**
 * 上报通道（03 §4 `trace_batch.channel`，int2）。
 *
 * - [REALTIME]：实时通道，小批（≤50 点）、满足触发条件即发，弱网也保发；
 * - [BACKFILL]：补传通道，历史积压点在 WiFi/充电时大批上传。
 *
 * 两个通道在限流上**独立计数**（06 §2.4 `rl:bc:{deviceId}:{channel}`），
 * 数值必须与协议一致，不可复用或重排。
 */
enum class Channel(
    val value: Short,
) {
    REALTIME(1),
    BACKFILL(2),
    ;

    companion object {
        fun from(value: Short): Channel =
            entries.firstOrNull { it.value == value }
                ?: throw IllegalArgumentException("未知上报通道: $value")
    }
}
