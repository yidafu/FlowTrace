package dev.yidafu.flowtrace.shared.cache.impl

import dev.yidafu.flowtrace.shared.cache.TombstoneStore
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class RedisTombstoneStore(
    private val redisTemplate: StringRedisTemplate,
) : TombstoneStore {
    override fun has(userId: String): Boolean = redisTemplate.hasKey(keyOf(userId)) ?: false

    override fun mark(
        userId: String,
        jobId: String,
        ttl: Duration,
    ) {
        redisTemplate.opsForValue().set(keyOf(userId), jobId, ttl)
    }

    fun keyOf(userId: String) = "tomb:u:$userId"
}
