package dev.yidafu.flowtrace.process.metrics

import dev.yidafu.flowtrace.process.clean.DropReason
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * 处理链路指标（docs/design/07-deploy-observability.md §4）。
 *
 * 覆盖两类信号：
 * - **吞吐与质量**：`ft_processor_points_total{result=inserted|dropped, reason}`，
 *   reason 取 [DropReason] 小写名（与 `BatchResult.droppedText()` 的日志口径一致），
 *   用于发现「某条清洗规则突然吃掉大量点」这类回归；
 * - **积压**：`ft_dirty_day_pending` / `ft_dirty_day_failed`（Gauge），由
 *   [dev.yidafu.flowtrace.process.recalc.DirtyDayRecalculator] 每轮扫描后刷新；
 *   `ft_processor_lag{partition}`（Gauge）由 [ConsumerLagProbe] 采样 broker 侧 offset 后刷新，
 *   口径是 `log-end-offset − 已提交 offset`。
 *
 * 死信计数 `ft_dlq_total{error_code}` 由 [dev.yidafu.flowtrace.process.dlq.DlqPublisher]
 * 调 [dlqProduced] 记——它是死信的唯一出口，比在监听器的三个分支各记一次更不容易漏。
 *
 * 为什么 Gauge 用 `AtomicLong` + 主动刷新，而不是在 `valueFunction` 里查库：
 * Micrometer 在**抓取时**才调用 valueFunction，若其中做 JDBC 查询，等于把 scrape 周期
 * 变成数据库压力源，且抓取超时会让指标变 NaN。定时刷新把「采样」和「抓取」解耦。
 *
 * **bean 名必须显式指定**：默认派生名 `processorMetrics` 与 Boot 的
 * `SystemMetricsAutoConfiguration.processorMetrics`（进程 CPU 指标 binder）重名，
 * 启动直接 `BeanDefinitionOverrideException`（Bean 覆盖默认关闭）。注入方一律按类型，
 * 所以改名对本文件之外没有影响。
 */
@Component("ftProcessorMetrics")
class ProcessorMetrics(
    private val registry: MeterRegistry,
) {
    private val dirtyPending = AtomicLong(0)
    private val dirtyFailed = AtomicLong(0)

    /** 分区 → 积压消息数；分区数在运行期不变，但首次采样才知道有几条，故懒注册。 */
    private val partitionLag = ConcurrentHashMap<Int, AtomicLong>()

    init {
        Gauge
            .builder(METRIC_DIRTY_PENDING, dirtyPending) { it.get().toDouble() }
            .description("待重算的 (user, day) 任务数")
            .register(registry)
        Gauge
            .builder(METRIC_DIRTY_FAILED, dirtyFailed) { it.get().toDouble() }
            .description("重算失败、等人工介入的任务数")
            .register(registry)
    }

    /** 清洗后实际入库的点数：`result=inserted, reason=none`。 */
    fun pointsInserted(count: Int) {
        if (count <= 0) return
        registry
            .counter(METRIC_POINTS, TAG_RESULT, RESULT_INSERTED, TAG_REASON, REASON_NONE)
            .increment(count.toDouble())
    }

    /** 被清洗规则丢弃的点：按原因分标签，一个原因一条计数。 */
    fun pointsDropped(dropped: Map<DropReason, Int>) {
        dropped.forEach { (reason, count) ->
            if (count <= 0) return@forEach
            registry
                .counter(METRIC_POINTS, TAG_RESULT, RESULT_DROPPED, TAG_REASON, reason.name.lowercase())
                .increment(count.toDouble())
        }
    }

    /** `ft_dlq_total{error_code}`：产生一条死信就记一次（投递成败都记，失败更严重）。 */
    fun dlqProduced(errorCode: String) {
        registry.counter(METRIC_DLQ, TAG_ERROR_CODE, errorCode).increment()
    }

    /** 刷新积压 Gauge；由重算扫描周期顺带调用（不另起定时任务）。 */
    fun refreshDirtyDay(
        pending: Long,
        failed: Long,
    ) {
        dirtyPending.set(pending)
        dirtyFailed.set(failed)
    }

    /**
     * 刷新「broker 侧积压」Gauge：`ft_processor_lag{partition}`，由
     * [dev.yidafu.flowtrace.process.metrics.ConsumerLagProbe] 用 AdminClient 采样后回填。
     *
     * 为什么不直接用 Kafka 客户端自带的 `kafka_consumer_fetch_manager_records_lag`：
     * 那个值是**消费线程自己 poll 时**才算的——消费卡住（毒丸消息无限重试、DB 抖动不退避）
     * 时它不再更新，lag 不涨反而不报警，正好错过最该报警的场景。AdminClient 直查 broker
     * 的 log-end-offset 与已提交 offset，与消费线程状态解耦。
     */
    fun refreshLag(
        partition: Int,
        lag: Long,
    ) {
        val holder =
            partitionLag.computeIfAbsent(partition) { id ->
                val value = AtomicLong(0)
                // 用 state object 而不是 valueFunction 查 broker：Micrometer 在抓取时才调
                // valueFunction，把远端查询塞进去等于让每个 scrape 打一次 Kafka。
                Gauge
                    .builder(METRIC_LAG, value) { it.get().toDouble() }
                    .description("该分区的积压消息数（log-end-offset − 已提交 offset）")
                    .tags(Tags.of(TAG_PARTITION, id.toString()))
                    .strongReference(true)
                    .register(registry)
                value
            }
        holder.set(lag)
    }

    private companion object {
        const val METRIC_POINTS = "ft.processor.points"
        const val METRIC_DLQ = "ft.dlq"
        const val METRIC_DIRTY_PENDING = "ft.dirty.day.pending"
        const val METRIC_DIRTY_FAILED = "ft.dirty.day.failed"
        const val METRIC_LAG = "ft.processor.lag"

        const val TAG_RESULT = "result"
        const val TAG_REASON = "reason"
        const val TAG_ERROR_CODE = "error_code"
        const val TAG_PARTITION = "partition"

        const val RESULT_INSERTED = "inserted"
        const val RESULT_DROPPED = "dropped"

        /**
         * 入库点没有「丢弃原因」。用固定值占位而不是空串：Prometheus 里空标签值会被当成
         * 「该标签不存在」，`sum by (reason)` 聚合时容易和真正缺失标签的序列混在一起。
         */
        const val REASON_NONE = "none"
    }
}
