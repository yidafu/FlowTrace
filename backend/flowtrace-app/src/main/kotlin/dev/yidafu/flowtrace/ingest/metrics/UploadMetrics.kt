package dev.yidafu.flowtrace.ingest.metrics

import dev.yidafu.flowtrace.shared.web.ApiErrorCode
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * 上报接入侧指标（docs/design/07-deploy-observability.md §4）。
 *
 * 只登记接入段的五类事实：请求结果分布、处理耗时、重复批、限流命中、Kafka 投递失败。
 * 挂在这里而不是散落到控制器里，是为了让指标名与标签只有一个事实源——改口径只改这个文件。
 *
 * 命名纪律（07 §4 的 Prometheus 口径 ↔ Micrometer 写法）：
 * - 代码里用**点号**、**不带 `_total` / `_seconds`**：`ft.upload.requests` 由 Prometheus registry
 *   自动转成 `ft_upload_requests_total`；写死后缀会变成 `..._total_total`；
 * - **标签只允许有限枚举**：`code` 取 [ApiErrorCode] 名，`channel` 取通道枚举名。
 *   `deviceId` / `batchId` / `userId` 一律不进标签——十万设备就是十万条时间序列，
 *   既压垮 Prometheus，也违反 07 §5「日志与指标不出坐标/不铺用户维度」的隐私口径。
 *
 * 线程安全：Micrometer 的 `counter(...)` 内部按 `Meter.Id` 缓存，多线程并发调用安全。
 */
@Component
class UploadMetrics(
    private val registry: MeterRegistry,
) {
    /** `ft_upload_requests_total{code}`：每个返回分支记一次（含 200 的 `OK`）。 */
    fun request(code: ApiErrorCode) {
        registry.counter(METRIC_REQUESTS, TAG_CODE, code.name).increment()
    }

    /** 成功返回（200）：`code=OK`。成功不是 [ApiErrorCode] 的成员，单独给一个稳定字符串。 */
    fun requestOk() {
        registry.counter(METRIC_REQUESTS, TAG_CODE, CODE_OK).increment()
    }

    /** `ft_upload_dup_batches_total`：幂等命中（同一 batchId 重复上报）。 */
    fun duplicateBatch() {
        registry.counter(METRIC_DUP_BATCHES).increment()
    }

    /** `ft_kafka_send_failures_total`：写 Kafka 抛出异常（已回滚幂等占位，本次返 5xx）。 */
    fun kafkaSendFailure() {
        registry.counter(METRIC_KAFKA_SEND_FAILURES).increment()
    }

    /** `ft_ratelimit_hits_total{channel}`：被限流拒绝的批（批数或点数超限都算一次）。 */
    fun rateLimited(channel: String) {
        registry.counter(METRIC_RATELIMIT_HITS, TAG_CHANNEL, channel).increment()
    }

    /**
     * `ft_upload_duration_seconds`：一批**成功受理**（已写 Kafka）的端到端处理耗时（07 §4，p99 > 100ms → P2）。
     *
     * 口径两点：
     * - **只记 200 + Kafka 确认这条路径**。422/413/429 是毫秒级快速拒绝，混进同一个直方图会把
     *   p99 往下拉，真正的慢批反而看不出；失败批的耗时由 `http_server_requests_seconds` 面板兜底。
     *   代价是「Kafka 打满 5s 超时才失败」这种最坏耗时不在这里体现——那条由
     *   `ft_kafka_send_failures_total` + 5xx 面板报警。
     * - **不加标签**：07 §4 的告警只按「上报耗时」一条口径算 p99；真要看实时/补传通道差异
     *   （补传单批最多 500 点，天然更慢），再加 `channel` 标签并用 `sum by (le, channel)` 分开算。
     *
     * 直方图桶写在**这里**而不是 application.yaml：桶边界必须跟 07 §4 的 100ms 告警对齐，
     * 和指标名放同一个文件才不会漂移（`http.server.requests` 的分位数靠 YAML 开关，少一行就
     * 整块面板空掉，这个坑不再复制一次）。
     */
    fun uploadDuration(elapsed: Duration) {
        Timer
            .builder(METRIC_DURATION)
            .description("上报批处理耗时（控制器入口 → Kafka 确认，不含网络传输与 gzip 解压）")
            // 导出 le 桶，否则 Prometheus 端只有 count/sum，histogram_quantile 算不出 p99
            .publishPercentileHistogram()
            // 显式钉住告警阈值对应的桶，保证 le="0.05"/"0.1"/"0.5"/"1" 一定存在
            .serviceLevelObjectives(SLO_50MS, SLO_100MS, SLO_500MS, SLO_1S)
            .register(registry)
            .record(elapsed)
    }

    private companion object {
        const val METRIC_REQUESTS = "ft.upload.requests"
        const val METRIC_DURATION = "ft.upload.duration"
        const val METRIC_DUP_BATCHES = "ft.upload.dup.batches"
        const val METRIC_KAFKA_SEND_FAILURES = "ft.kafka.send.failures"
        const val METRIC_RATELIMIT_HITS = "ft.ratelimit.hits"

        /** 与 07 §4 的 p99 > 100ms 告警、application.yaml 里 http.server.requests 的 SLO 同口径。 */
        val SLO_50MS: Duration = Duration.ofMillis(50)
        val SLO_100MS: Duration = Duration.ofMillis(100)
        val SLO_500MS: Duration = Duration.ofMillis(500)
        val SLO_1S: Duration = Duration.ofSeconds(1)

        const val TAG_CODE = "code"
        const val TAG_CHANNEL = "channel"

        /** 与 04 §1.4 错误码并列展示，但表示成功。 */
        const val CODE_OK = "OK"
    }
}
