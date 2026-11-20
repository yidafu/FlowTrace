package dev.yidafu.flowtrace.shared.messaging

/**
 * Kafka topic 常量（06 §7）。
 *
 * 主 topic 名由 `flowtrace.upload.topic` 配置注入生产者（upload 与 processor 共用同一个键，
 * 避免两处写死不一致）；这里只给**默认值**与 DLQ 常量，供代码里需要字面量的地方引用。
 */
object Topics {
    /** 轨迹批主 topic 的默认名（06 §7；实际上报链路按配置注入）。 */
    const val TRACE_BATCH = "trace.batch.v1"

    /** 死信 topic（06 §3.7）：原消息字节 + `error.code` 头，管理员重放用。 */
    const val TRACE_BATCH_DLQ = "trace.batch.dlq.v1"
}
