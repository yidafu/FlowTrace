package dev.yidafu.flowtrace.ingest.service

import dev.yidafu.flowtrace.ingest.config.UploadProperties
import dev.yidafu.flowtrace.ingest.metrics.UploadMetrics
import dev.yidafu.flowtrace.shared.cache.FlowtraceCache
import dev.yidafu.flowtrace.v1.Channel
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * 设备级限流：固定 60s 窗口内的「批次数」与「点数」双重配额（04 §3、07 §3）。
 *
 * - 批数上限按通道区分：实时 [UploadProperties.limitBatchPerMinRt]、补传 [UploadProperties.limitBatchPerMinBf]；
 *   未指明通道（`CHANNEL_UNSPECIFIED`）按实时档从紧处理。计数键带 channel（06 §2.4 `rl:bc:{deviceId}:{channel}`），
 *   两通道各自独立吃配额，互不挤占。
 * - 点数上限 [UploadProperties.limitPointsPerMin] 两通道共享同一份「设备点数额度」（`rl:pc:{deviceId}`）。
 * - **计数键必须带 deviceId**：键内缺设备维度会让配额退化为全平台共享，
 *   一台设备打满即可拖死所有设备。
 *
 * 类名沿用既有拼写 `ReteLimiter`（语义即 RateLimiter），改名需同步控制器与测试的引用。
 */
@Component
class ReteLimiter(
    private val cache: FlowtraceCache,
    private val props: UploadProperties,
    private val metrics: UploadMetrics,
) {
    /**
     * 判定本批是否放行，**同时消耗配额**（不是纯查询，重复调用会重复计数）。
     *
     * 顺序即语义：先扣批数、再扣点数。批数已超限时直接返回 false 且**不再累计点数**——
     * 被拒的这批不占点数额度，设备恢复正常节奏后不会因为「被拒的批也算点数」而继续被卡。
     *
     * @param pointCount 批内点数（已过 upload 的字段校验）；<= 0（空批）短路放行，
     *   因为 [FlowtraceCache.incrementBy] 要求步长为正
     * @return true = 放行；false = 超限，调用方回 429 + Retry-After
     */
    fun check(
        deviceId: String,
        channel: Channel,
        pointCount: Int,
    ): Boolean {
        val batchLimit =
            when (channel) {
                Channel.CHANNEL_BACKFILL -> props.limitBatchPerMinBf
                else -> props.limitBatchPerMinRt
            }
        val batchCount = cache.increment(RATE_LIMIT_BATCH_COUNT + deviceId + ":" + channel.name, WINDOW)
        if (batchLimit < batchCount) {
            // 指标只记「被拒」，不记 deviceId：设备维度的时间序列会无限膨胀（07 §4 标签纪律）
            metrics.rateLimited(channel.name)
            return false
        }
        // 空批不占点数额度：incrementBy 要求步长为正，先短路
        if (pointCount <= 0) {
            return true
        }
        val nextPointCount = cache.incrementBy(RATE_LIMIT_POINT_COUNT + deviceId, pointCount.toLong(), WINDOW)
        if (props.limitPointsPerMin < nextPointCount) {
            metrics.rateLimited(channel.name)
            return false
        }
        return true
    }

    companion object {
        private const val RATE_LIMIT_BATCH_COUNT = "rl:bc:"
        private const val RATE_LIMIT_POINT_COUNT = "rl:pc:"

        /** 限流窗口：键名 `*-per-min` 已把语义钉死为分钟，故不开放配置（07 §3）。 */
        private val WINDOW: Duration = Duration.ofSeconds(60)
    }
}
