package dev.yidafu.flowtrace.shared.messaging

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory

/**
 * Kafka 生产者配置（06 §1.3）：acks=all + enable.idempotence=true 保证重试不重复写，
 * lz4 压缩、linger.ms=5 批处理降低小批上报的请求数。
 *
 * key = deviceId（String），value = TraceBatchMessage 的 protobuf 字节（ByteArray），
 * 与 Boot 自动配置的 Object/Object KafkaTemplate 区分，按类型注入即可命中本 bean。
 */
@Configuration(proxyBeanMethods = false)
class TraceKafkaProducerConfig {
    @Bean
    fun traceProducerFactory(
        @Value("\${spring.kafka.bootstrap-servers:localhost:9092}") bootstrapServers: String,
        // Boot 的 KafkaMetricsAutoConfiguration 只把 kafka_producer_* 指标（发送速率、重试、错误数）
        // 通过 customizer 挂到它自己建的工厂上；本类是手工工厂，要显式接一遍。
        // 未启 actuator（没有 MeterRegistry）时 provider 为空，不影响启动。
        factoryCustomizers: ObjectProvider<DefaultKafkaProducerFactoryCustomizer>,
    ): ProducerFactory<String, ByteArray> {
        val props =
            mapOf<String, Any>(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                ProducerConfig.ACKS_CONFIG to "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
                ProducerConfig.COMPRESSION_TYPE_CONFIG to "lz4",
                ProducerConfig.LINGER_MS_CONFIG to 5,
            )
        val factory = DefaultKafkaProducerFactory<String, ByteArray>(props)
        factoryCustomizers.orderedStream().forEach { it.customize(factory) }
        return factory
    }

    @Bean
    fun traceKafkaTemplate(producerFactory: ProducerFactory<String, ByteArray>): KafkaTemplate<String, ByteArray> =
        KafkaTemplate(producerFactory)
}
