package dev.yidafu.flowtrace.process.dlq

/**
 * 死信投递出口（06 §3.7）。
 *
 * 抽成接口的目的只有一个：让 `TraceBatchListener` 的用例不必启动 Kafka 也能断言
 * 「这批判定失败时到底投没投死信、错误码是什么」。
 */
interface DlqSink {
    /**
     * @param key 分区键（通常 deviceId；解析失败时可能为 null）
     * @param payload 原消息字节（原样保留，便于 admin 重投）
     * @param errorCode 机器可读错误码（落 `error.code` 头）
     * @param errorMessage 人读原因（落 `error.msg` 头）
     * @param batchId 批 ID（解析失败时为空）
     * @param channel 上报通道（解析失败时为空）
     */
    fun publish(
        key: String?,
        payload: ByteArray,
        errorCode: String,
        errorMessage: String?,
        batchId: String? = null,
        channel: Int? = null,
    )
}
