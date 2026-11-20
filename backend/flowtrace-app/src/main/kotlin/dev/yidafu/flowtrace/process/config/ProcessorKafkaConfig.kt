package dev.yidafu.flowtrace.process.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.ExponentialBackOff

/**
 * processor 侧的 Kafka 消费装配（08 §4、§11）。
 *
 * 为什么不用 Boot 自动配置的 `kafkaListenerContainerFactory`：本模块要的两件事自动化都给不了——
 * ① `AckMode.MANUAL`，由 `TraceBatchListener` 在处理成功后显式 `acknowledge()`（失败即不 ack，
 * offset 不前进）；② 自定义「可重试异常永不放弃」的错误处理器。两者都在本类手工装配，
 * 因此 `spring.kafka.*` 只提供地址兜底，真正的消费参数以 [ProcessorProperties] 为准。
 *
 * bean 名即契约：`TraceBatchListener` 的 `@KafkaListener(containerFactory = "traceProcessorContainerFactory")`
 * 按名查找，改名即断链（启动期不报错，只在启动后收不到消息）。
 */
@Configuration(proxyBeanMethods = false)
class ProcessorKafkaConfig(
    private val props: ProcessorProperties,
) {
    /**
     * 消费工厂（key=String, value=原始字节）。
     *
     * value 保持 `ByteArray` 而不是反序列化成对象：protobuf 解码失败要能在监听器里被当成
     * 「确定性失败 → ack 丢弃」处理，交给反序列化器会变成可重试异常反而卡分区。
     *
     * `enable.auto.commit=false` 是 MANUAL ack 的前提——自动提交会和显式 ack 抢 offset；
     * `auto.offset.reset=earliest` 保证新消费组首次上线不跳过积压（重放由 pointId/segmentId
     * 的确定性派生吸收）。
     */
    @Bean
    fun traceProcessorConsumerFactory(
        // Kafka 客户端指标（kafka_consumer_*，含 records-lag-max）由 Boot 的
        // KafkaMetricsAutoConfiguration 以 customizer 形式提供，但它只 apply 到自己建的工厂上。
        // 本类手工建工厂（见类注释），必须显式接一遍，否则 lag 面板永远是空的。
        // 容器里没有 MeterRegistry（未启 actuator）时该 provider 为空，不影响启动。
        factoryCustomizers: ObjectProvider<DefaultKafkaConsumerFactoryCustomizer>,
    ): ConsumerFactory<String, ByteArray> {
        val factory =
            DefaultKafkaConsumerFactory<String, ByteArray>(
                mapOf(
                    BOOTSTRAP_SERVERS_CONFIG to props.bootstrapServers,
                    GROUP_ID_CONFIG to props.consumerGroup,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                    VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG to props.pollRecords,
                    // 5min：单批墙钟上限只有 batchTimeoutMs（60s），留足「写库 + 分段 + 退避重试」余量，
                    // 又能在消费线程卡死时靠 rebalance 把分区交出去
                    // 必须给 Int：Kafka ConfigDef 里该键是 32 位整型，传 Long 会在 consumer 创建时
                    // 抛 ConfigException（Expected value to be a 32-bit integer）
                    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG to 300_000,
                    // 单次 fetch 上限 1MiB（默认 50MiB）：端侧单批 ≤500 点，展开成内部消息也就几十 KB。
                    // 与 max.poll.records 一起把「一次 poll 的工作量」框住——积压时不会一次拉进几十 MB
                    // 才被 max.poll.records 截断，单批处理时长因而更可控
                    ConsumerConfig.FETCH_MAX_BYTES_CONFIG to (1 shl 20),
                ),
            )
        factoryCustomizers.orderedStream().forEach { it.customize(factory) }
        return factory
    }

    /**
     * 监听容器工厂：并发数 = 分区数（[ProcessorProperties.concurrency]），ack 手动。
     *
     * 并发大于分区数只会空转；小于分区数则部分分区排在后头消费，实时性变差。
     */
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
     * 退避：起值取 `flowtrace.processor.retry-backoff-ms`（默认 2s）、×2、封顶 60s。
     * `maxElapsedTime` 默认只有 15s，到点就 STOP 走跳过分支，必须显式放开。
     */
    private fun traceProcessorErrorHandler(): DefaultErrorHandler {
        val backOff =
            ExponentialBackOff(props.retryBackoffMs, 2.0).apply {
                maxInterval = MAX_RETRY_INTERVAL_MS
                maxElapsedTime = Long.MAX_VALUE
            }
        return DefaultErrorHandler(backOff)
    }

    private companion object {
        /** 单次重试间隔上限：再长会让「DB 恢复后」的追平速度慢到不可接受。 */
        const val MAX_RETRY_INTERVAL_MS = 60_000L
    }
}
