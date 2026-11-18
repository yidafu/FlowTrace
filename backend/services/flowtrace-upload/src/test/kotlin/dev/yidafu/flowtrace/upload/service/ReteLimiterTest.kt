package dev.yidafu.flowtrace.upload.service

import dev.yidafu.flowtrace.common.cache.FlowtraceCache
import dev.yidafu.flowtrace.upload.config.UploadProperties
import dev.yidafu.flowtrace.v1.Channel
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * 限流器测试：覆盖配置化后的三处契约。
 *
 * - 批数阈值按通道分离（实时 12 / 补传 6），且兼容 `CHANNEL_UNSPECIFIED`；
 * - 点数配额按设备隔离（键含 deviceId），一台设备打满不牵连其他设备；
 * - 批数与点数各自独立计数，互不吃额度。
 */
class ReteLimiterTest {
    private val props = UploadProperties()
    private val cache = InMemoryCache()
    private val limiter = ReteLimiter(cache, props)

    @Test
    fun `实时通道允许 12 批，第 13 批拒绝`() {
        repeat(props.limitBatchPerMinRt) {
            assertTrue(limiter.check("dev-rt", Channel.CHANNEL_REALTIME, 1), "第 $it 批应放行")
        }
        assertFalse(limiter.check("dev-rt", Channel.CHANNEL_REALTIME, 1), "第 13 批应被限流")
    }

    @Test
    fun `补传通道阈值更紧，第 7 批拒绝`() {
        repeat(props.limitBatchPerMinBf) {
            assertTrue(limiter.check("dev-bf", Channel.CHANNEL_BACKFILL, 1), "第 $it 批应放行")
        }
        assertFalse(limiter.check("dev-bf", Channel.CHANNEL_BACKFILL, 1), "第 7 批应被限流")
    }

    @Test
    fun `未指明通道按实时档从紧处理`() {
        repeat(props.limitBatchPerMinRt) {
            assertTrue(limiter.check("dev-unspec", Channel.CHANNEL_UNSPECIFIED, 1))
        }
        assertFalse(limiter.check("dev-unspec", Channel.CHANNEL_UNSPECIFIED, 1))
    }

    @Test
    fun `点数配额按设备隔离`() {
        // dev-a 一次打满分钟点数额度，随后被拒
        assertTrue(limiter.check("dev-a", Channel.CHANNEL_REALTIME, props.limitPointsPerMin))
        assertFalse(limiter.check("dev-a", Channel.CHANNEL_REALTIME, 1))
        // dev-b 不受影响（旧实现键内缺 deviceId，此处会误伤）
        assertTrue(limiter.check("dev-b", Channel.CHANNEL_REALTIME, props.limitPointsPerMin))
        assertEquals(6000L, cache.counters["rl:pc:dev-b"])
    }

    @Test
    fun `批数与点数独立计数`() {
        // 点数打满后批数仍只记了 1 次，不会因点数超限而虚增批数
        assertTrue(limiter.check("dev-1", Channel.CHANNEL_REALTIME, props.limitPointsPerMin))
        assertFalse(limiter.check("dev-1", Channel.CHANNEL_REALTIME, 1))
        assertEquals(2L, cache.counters[batchKey("dev-1", Channel.CHANNEL_REALTIME)])
        assertEquals(6001L, cache.counters["rl:pc:dev-1"])
    }

    @Test
    fun `两通道批数各自独立计数`() {
        // 补传通道吃满 6 批，不占用实时通道的 12 批配额（key 带 channel，06 §2.4）
        repeat(props.limitBatchPerMinBf) {
            assertTrue(limiter.check("dev-mix", Channel.CHANNEL_BACKFILL, 1))
        }
        assertFalse(limiter.check("dev-mix", Channel.CHANNEL_BACKFILL, 1))
        repeat(props.limitBatchPerMinRt) {
            assertTrue(limiter.check("dev-mix", Channel.CHANNEL_REALTIME, 1))
        }
        // 被拒的那次同样自增（固定窗口：n>limit 后本分钟持续 429，06 §2.4），故为 7
        assertEquals(7L, cache.counters[batchKey("dev-mix", Channel.CHANNEL_BACKFILL)])
        assertEquals(12L, cache.counters[batchKey("dev-mix", Channel.CHANNEL_REALTIME)])
    }

    @Test
    fun `空批不占点数额度`() {
        repeat(props.limitBatchPerMinRt) {
            assertTrue(limiter.check("dev-empty", Channel.CHANNEL_REALTIME, 0))
        }
        assertNull(cache.counters["rl:pc:dev-empty"], "点数为 0 不应写入点数额度键")
    }

    /** 批次计数键（06 §2.4：`rl:bc:{deviceId}:{channel}`）。 */
    private fun batchKey(
        deviceId: String,
        channel: Channel,
    ) = "rl:bc:$deviceId:${channel.name}"

    /** 内存版 [FlowtraceCache]：只实现计数语义，窗口过期由用例自行控制（本测试不依赖过期）。 */
    private class InMemoryCache : FlowtraceCache {
        val counters = mutableMapOf<String, Long>()

        override fun <T : Any> get(
            key: String,
            type: Class<T>,
        ): T? = null

        override fun put(
            key: String,
            value: Any,
            ttl: Duration?,
        ) = Unit

        override fun evict(key: String) {
            counters.remove(key)
        }

        override fun incrementBy(
            key: String,
            delta: Long,
            ttl: Duration?,
        ): Long {
            require(delta > 0) { "incrementBy 步长必须为正数: delta=$delta, key=$key" }
            val next = (counters[key] ?: 0L) + delta
            counters[key] = next
            return next
        }
    }
}
