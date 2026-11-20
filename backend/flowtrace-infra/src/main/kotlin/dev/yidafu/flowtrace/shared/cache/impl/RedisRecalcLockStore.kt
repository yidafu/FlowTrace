package dev.yidafu.flowtrace.shared.cache.impl

import dev.yidafu.flowtrace.shared.cache.RecalcLockStore
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * `recalc:u:{userId}` 的 Redis 实现（06 §3.6）。
 *
 * 加锁走 `SET key 1 NX PX ttl`（Spring Data 的 `setIfAbsent(key, value, timeout)`），
 * 一次往返完成「不存在才写 + 设过期」，不存在 `has` 与 `put` 之间的竞态窗口——
 * 这个窗口正是旧实现对多实例重算无能为力的原因。
 *
 * **TTL 是必需的**：进程在重算中途崩溃时不会有人来 unlock，没有 TTL 就留下永久锁，
 * 该用户之后的每一批都跳过分段，直到人工介入。
 */
@Component
class RedisRecalcLockStore(
    private val redis: StringRedisTemplate,
) : RecalcLockStore {
    override fun tryLock(
        userId: String,
        ttl: Duration,
    ): Boolean = redis.opsForValue().setIfAbsent(keyOf(userId), LOCK_VALUE, ttl) == true

    override fun isLocked(userId: String): Boolean = redis.hasKey(keyOf(userId))

    override fun unlock(userId: String) {
        redis.delete(keyOf(userId))
    }

    fun keyOf(userId: String) = "recalc:u:$userId"

    private companion object {
        /** 锁值只需存在，不承载语义；固定值便于 `SCAN` 排查时肉眼识别。 */
        const val LOCK_VALUE = "1"
    }
}
