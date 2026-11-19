package dev.yidafu.flowtrace.processor.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.ExponentialBackOff

@Configuration(proxyBeanMethods = false)
class ProcessorKafkaConfig(
    private val props: ProcessorProperties,
) {
    @Bean
    fun traceProcessorConsumerFactory(): ConsumerFactory<String, ByteArray> =
        DefaultKafkaConsumerFactory(
            mapOf(
                BOOTSTRAP_SERVERS_CONFIG to props.bootstrapServers,
                GROUP_ID_CONFIG to props.consumerGroup,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG to props.pollRecords,
                // 必须给 Int：Kafka ConfigDef 里该键是 32 位整型，传 Long 会在 consumer 创建时
                // 抛 ConfigException（Expected value to be a 32-bit integer）
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG to 300_000,
                ConsumerConfig.FETCH_MAX_BYTES_CONFIG to (1 shl 20),
            ),
        )

    @Bean
    fun traceProcessorContainerFactory(cf: ConsumerFactory<String, ByteArray>) =
        ConcurrentKafkaListenerContainerFactory<String, ByteArray>().apply {
            setConsumerFactory(cf)
            setConcurrency(props.concurrency)
            containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
            setCommonErrorHandler(traceProcessorErrorHandler())
        }

    /**
     * 可重试异常「永不放弃」：不给 recoverer + 退避没有上限，容器会 seek 回原 offset 反复重试，
     * 期间该分区不前进（对齐 08 §11「可重试异常阻塞分区」的语义）。
     *
     * 为什么不沿用 Spring 的默认处理器（`CommonLoggingErrorHandler` 等价于重试 10 次后 log 并
     * 跳过）：跳过会提交 offset 把消息丢掉——DB/Redis 抖一次就静默丢一批，与「至少一次投递 +
     * 幂等写」的交付语义冲突。毒丸消息不靠这里兜底，由 `BatchPipeline` 按 `batchTimeoutMs`
     * 判定后转 DLQ，所以此处可以放心无限重试。
     *
     * 退避：5s 起、×2、封顶 60s。`maxElapsedTime` 默认只有 15s，到点就 STOP 走跳过分支，
     * 必须显式放开。
     */
    private fun traceProcessorErrorHandler(): DefaultErrorHandler {
        val backOff =
            ExponentialBackOff(5_000L, 2.0).apply {
                maxInterval = 60_000L
                maxElapsedTime = Long.MAX_VALUE
            }
        return DefaultErrorHandler(backOff)
    }
}
