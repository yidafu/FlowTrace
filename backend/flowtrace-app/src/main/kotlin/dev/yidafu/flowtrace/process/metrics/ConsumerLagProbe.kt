package dev.yidafu.flowtrace.process.metrics

import dev.yidafu.flowtrace.process.config.ProcessorProperties
import dev.yidafu.flowtrace.shared.messaging.Topics
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * 消费积压采样器：`ft_processor_lag{partition}` 的数据来源（07 §4）。
 *
 * **为什么不直接用 Kafka 客户端自带的 `records_lag`**：那个值由消费线程在 poll 时按
 * fetch 结果算出来——消费线程一旦卡住（毒丸消息无限重试、DB 抖动不退避、分区被 seek
 * 回原 offset），它就不再更新。于是「最需要 lag 报警」的时刻，指标反而不涨甚至消失。
 * 本类用 AdminClient 直查 broker 的 `log-end-offset` 与已提交 offset，和消费线程状态解耦：
 * 消费停了，这里的数字照样往上走。
 *
 * 采样周期默认 15s，与 Prometheus 抓取间隔对齐；一轮没跑完（broker 慢/超时）就跳过本轮，
 * 不排队堆积请求。失败只 WARN 并保留上一轮的值——Gauge 归零会被误读成「积压清零」。
 *
 * 线程模型：`@Scheduled` 只负责触发，真正干活的是协程 + `Dispatchers.IO`（AdminClient 全阻塞）。
 */
@Component
class ConsumerLagProbe(
    private val props: ProcessorProperties,
    private val metrics: ProcessorMetrics,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /** 上一轮尚未结束时置位，避免 broker 慢时把采样请求堆起来。 */
    private val sampling = AtomicBoolean(false)

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    /** 懒建：AdminClient 首次调用才连 broker，采样失败不影响应用启动。 */
    private val admin: Admin by lazy {
        Admin.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to props.bootstrapServers))
    }

    @Scheduled(
        fixedDelayString = $$"${flowtrace.processor.lag-probe-interval-sec:15}",
        timeUnit = TimeUnit.SECONDS,
    )
    fun schedule() {
        if (!sampling.compareAndSet(false, true)) return
        scope.launch {
            try {
                sample()
            } catch (ex: Exception) {
                log.warn("processor 积压采样失败，保留上一轮值 cause={}", ex.message)
            } finally {
                sampling.set(false)
            }
        }
    }

    private suspend fun sample() =
        withContext(Dispatchers.IO) {
            val topic = Topics.TRACE_BATCH
            val partitions =
                admin
                    .describeTopics(listOf(topic))
                    .allTopicNames()
                    .get(TIMEOUT_SEC, TimeUnit.SECONDS)[topic]
                    ?.partitions()
                    ?.map { TopicPartition(topic, it.partition()) }
                    .orEmpty()
            if (partitions.isEmpty()) return@withContext

            val endOffsets =
                admin
                    .listOffsets(partitions.associateWith { OffsetSpec.latest() })
                    .all()
                    .get(TIMEOUT_SEC, TimeUnit.SECONDS)

            // 消费组刚建／位点被清过时查不到提交，此时把已提交当 0，积压 = 全量未消费，
            // 宁可报大也不要把「没位点」误判成「没积压」。
            val committed =
                runCatching {
                    admin
                        .listConsumerGroupOffsets(props.consumerGroup)
                        .partitionsToOffsetAndMetadata()
                        .get(TIMEOUT_SEC, TimeUnit.SECONDS)
                }.getOrElse { ex ->
                    log.debug("消费组 {} 暂无提交位点，积压按未消费计 cause={}", props.consumerGroup, ex.message)
                    emptyMap()
                }

            partitions.forEach { tp ->
                val end = endOffsets[tp]?.offset() ?: return@forEach
                val acknowledged = committed[tp]?.offset() ?: 0L
                metrics.refreshLag(tp.partition(), (end - acknowledged).coerceAtLeast(0))
            }
        }

    @PreDestroy
    fun close() {
        runCatching { admin.close(Duration.ofSeconds(5)) }
    }

    private companion object {
        /** 单次 Admin 调用上限：超过就放弃这一轮，等下个周期。 */
        const val TIMEOUT_SEC = 5L
    }
}
