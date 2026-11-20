package dev.yidafu.flowtrace.shared.cache.impl

import dev.yidafu.flowtrace.shared.cache.IdempotencyGate
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * `idem:batch:{batchId}` 的 Redis 实现（06 §2.2 第 7 步 / D19）。
 *
 * `SET key 1 NX PX 7d` 一次往返完成「不存在才写 + 设过期」：
 * - `NX` 保证并发重试只有一次成功（幂等的判定点）；
 * - `PX` 保证占位不会永久残留——批次被拒/进程崩溃后，同一 batchId 的合法重试仍能通过。
 *
 * TTL 取 7 天：与端侧补传队列上限（7 天）对齐，超过这个窗口的重复批已无重试可能，
 * 占位过期不会造成「重复入库」（点级 `pointId` 还有一层主键幂等）。
 *
 * Redis 不可用时**抛异常**（不降级）：放行会把“查不到”当成“首次”，重复批会被重复写 Kafka。
 * 06 §2.6 的降级方案（先查 `trace_batch` 再插）尚未实现，届时也应在 upload 侧判定，而不是在这里猜。
 */
@Component
class RedisIdempotencyGate(
    private val redis: StringRedisTemplate,
) : IdempotencyGate {
    override fun tryAcquire(key: String): Boolean = redis.opsForValue().setIfAbsent(keyOf(key), OCCUPIED, TTL) == true

    override fun release(key: String) {
        redis.delete(keyOf(key))
    }

    fun keyOf(key: String) = "idem:batch:$key"

    private companion object {
        /** 占位值只表示「已受理」，不承载语义。 */
        const val OCCUPIED = "1"

        /** 7 天：覆盖端侧补传窗口（AGENTS「本地队列上限 7 天」）。 */
        val TTL: Duration = Duration.ofDays(7)
    }
}
