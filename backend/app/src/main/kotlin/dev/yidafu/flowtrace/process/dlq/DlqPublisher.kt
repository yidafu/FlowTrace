package dev.yidafu.flowtrace.process.dlq

import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

/**
 * 死信投递（06 §3.7 / 08 §11）。
 *
 * 什么消息进 DLQ：**确定性失败**——同一串字节重投一万次结果都一样（protobuf 解析失败、
 * 批头非法、毒丸超时）。DB/Redis 抖动这类可重试异常**不进** DLQ，由容器的无限退避重试挡住，
 * 否则一次 PG 重启会把上千条正常消息打成死信。
 *
 * 消息体约定（06 §3.7）：**原 value 字节原样保留** + 头
 * `error.code` / `error.msg` / `bId` / `ch`，key 仍是 deviceId（保持可回溯分区）。
 * 原字节不动的原因：DLQ 里的东西要能被 admin 原样重投回主 topic（batchId 幂等天然防重）。
 *
 * 发送失败不抛给调用方：DLQ 是「兜底通道」，它自己挂了不该让已经确认失败的批次再卡住分区；
 * 只记 ERROR，靠日志告警人工介入（此时原始 offset 已 ack，消息只剩日志里这一份）。
 */
@Component
class DlqPublisher(
    private val kafkaTemplate: KafkaTemplate<String, ByteArray>,
    @Value($$"${flowtrace.processor.dlq-topic:trace.batch.dlq.v1}") private val topic: String,
    @Value($$"${flowtrace.kafka.send-timeout-sec:5}") private val sendTimeoutSec: Long,
) : DlqSink {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 投递一条死信（同步等待 broker 确认）。
     *
     * @param key 分区键，通常是 deviceId；解析失败时可能为 null（Kafka 会按轮询分区）
     * @param payload 原消息字节
     * @param errorCode 机器可读错误码（`INVALID_PROTOBUF` / `BATCH_TIMEOUT` / …）
     * @param errorMessage 人读原因（截断到 512 字符，避免异常栈把消息撑大）
     * @param batchId 批 ID（可空：解析失败的坏消息拿不到）
     * @param channel 通道（可空）
     */
    override fun publish(
        key: String?,
        payload: ByteArray,
        errorCode: String,
        errorMessage: String?,
        batchId: String?,
        channel: Int?,
    ) {
        // 显式声明类型：key 可为 null（解析失败的坏消息没有分区键），
        // 不写死类型的话 Kotlin 会推断成 ProducerRecord<String?, ByteArray>，与 KafkaTemplate<String, ByteArray> 不匹配
        val record: ProducerRecord<String, ByteArray> = ProducerRecord(topic, key, payload)
        record.headers().add(HEADER_ERROR_CODE, errorCode.toByteArray(Charsets.UTF_8))
        errorMessage?.let { record.headers().add(HEADER_ERROR_MSG, it.take(MAX_MSG_CHARS).toByteArray(Charsets.UTF_8)) }
        batchId?.let { record.headers().add(HEADER_BATCH_ID, it.toByteArray(Charsets.UTF_8)) }
        channel?.let { record.headers().add(HEADER_CHANNEL, it.toString().toByteArray(Charsets.UTF_8)) }
        try {
            kafkaTemplate.send(record).get(sendTimeoutSec, TimeUnit.SECONDS)
            log.warn(
                "processor 消息已投死信 topic={} key={} batchId={} code={} bytes={}",
                topic,
                key,
                batchId,
                errorCode,
                payload.size,
            )
        } catch (ex: Exception) {
            log.error(
                "processor 死信投递失败（原始消息已丢弃，仅剩本条日志） key={} batchId={} code={} bytes={} cause={}",
                key,
                batchId,
                errorCode,
                payload.size,
                ex.message,
            )
        }
    }

    private companion object {
        const val HEADER_ERROR_CODE = "error.code"
        const val HEADER_ERROR_MSG = "error.msg"
        const val HEADER_BATCH_ID = "bId"
        const val HEADER_CHANNEL = "ch"
        const val MAX_MSG_CHARS = 512
    }
}
