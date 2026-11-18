package dev.yidafu.flowtrace.messaging

/**
 * Kafka topic 常量（06 §7）。主 topic 名由 flowtrace.upload.topic 配置注入生产者。
 */
object Topics {
    const val TRACE_BATCH_DLQ = "trace.batch.dlq.v1"
}
