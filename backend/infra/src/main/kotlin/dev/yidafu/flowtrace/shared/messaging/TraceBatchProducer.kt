package dev.yidafu.flowtrace.shared.messaging

import dev.yidafu.flowtrace.internal.v1.TraceMessage
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

/**
 * 轨迹批写入 Kafka（06 §1.3 / §7）。
 *
 * topic 由 [flowtrace.upload.topic] 配置注入；key = deviceId 保证同设备有序；
 * value = TraceBatchMessage 绝对值 protobuf 字节。消息头 bId（batchId 原样 UUIDv7 字符串，
 * 02 §6 注释「十进制字符串」为笔误）/ ch（通道 1|2）。
 *
 * send().get() 同步等待 broker 确认，失败向上抛异常——由调用方（upload）回滚幂等 key 并返回 5xx，
 * 不允许 fire-and-forget（否则「返回 200 但 Kafka 未写」）。
 */
@Component
class TraceBatchProducer(
    private val kafkaTemplate: KafkaTemplate<String, ByteArray>,
    @Value($$"${flowtrace.upload.topic:trace.batch.v1}") private val topic: String,
    @Value($$"${flowtrace.kafka.send-timeout-sec:5}") private val sendTimeoutSec: Long,
) {
    fun send(
        deviceId: String,
        message: TraceMessage.TraceBatchMessage,
    ) {
        val record =
            ProducerRecord(
                topic,
                deviceId,
                message.toByteArray(),
            ).apply {
                headers().add(HEADER_BATCH_ID, message.batchId.toByteArray(Charsets.UTF_8))
                headers().add(HEADER_CHANNEL, message.channel.toString().toByteArray(Charsets.UTF_8))
            }
        kafkaTemplate.send(record).get(sendTimeoutSec, TimeUnit.SECONDS)
    }

    private companion object {
        const val HEADER_BATCH_ID = "bId"
        const val HEADER_CHANNEL = "ch"
    }
}
