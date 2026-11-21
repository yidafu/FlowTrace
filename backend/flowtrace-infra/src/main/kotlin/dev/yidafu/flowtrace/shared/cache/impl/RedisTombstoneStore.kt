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

    /**
     * MGET 一次覆盖全部入参（[TombstoneStore.filterTombstoned] 契约）。
     *
     * 值语义与 [has] 的 EXISTS 等价：墓碑值是 jobId 字符串，键存在则值必非 null，
     * 所以「GET 返回 null」即「墓碑不存在」。异常直接抛出——降级方向（隐私优先整 cycle 重试）
     * 由调用方（processor 的 [BatchPipeline][dev.yidafu.flowtrace.process.pipeline.BatchPipeline]）决定。
     */
    override fun filterTombstoned(userIds: Collection<String>): Set<String> {
        if (userIds.isEmpty()) return emptySet()
        val distinct = userIds.distinct()
        val values: List<String?> = redisTemplate.opsForValue().multiGet(distinct.map { keyOf(it) })
        return distinct.filterIndexed { i, _ -> values[i] != null }.toSet()
    }

    override fun mark(
        userId: String,
        jobId: String,
        ttl: Duration,
    ) {
        redisTemplate.opsForValue().set(keyOf(userId), jobId, ttl)
    }

    fun keyOf(userId: String) = "tomb:u:$userId"
}
