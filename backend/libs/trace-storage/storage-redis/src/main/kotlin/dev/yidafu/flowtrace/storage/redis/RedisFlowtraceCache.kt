package dev.yidafu.flowtrace.storage.redis

import dev.yidafu.flowtrace.common.cache.FlowtraceCache
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.time.Duration

/**
 * [FlowtraceCache] 的 Redis 实现：StringRedisTemplate + Jackson JSON 文本值。
 *
 * 由 flowtrace-app 组件扫描装配；Redis 连接由 Lettuce 惰性建立，
 * 本地无 Redis 实例不影响应用启动（首次访问才建连）。
 */
@Component
class RedisFlowtraceCache(
    private val redisTemplate: StringRedisTemplate,
    private val objectMapper: ObjectMapper,
) : FlowtraceCache {
    override fun <T : Any> get(
        key: String,
        type: Class<T>,
    ): T? {
        val json = redisTemplate.opsForValue().get(key) ?: return null
        return objectMapper.readValue(json, type)
    }

    override fun put(
        key: String,
        value: Any,
        ttl: Duration?,
    ) {
        val json = objectMapper.writeValueAsString(value)
        val ops = redisTemplate.opsForValue()
        if (ttl != null) ops.set(key, json, ttl) else ops.set(key, json)
    }

    override fun evict(key: String) {
        redisTemplate.delete(key)
    }

    override fun incrementBy(
        key: String,
        delta: Long,
        ttl: Duration?,
    ): Long {
        require(delta > 0) { "incrementBy 步长必须为正数: delta=$delta, key=$key" }
        // INCRBY 与首次 PEXPIRE 打包为 Lua，在 Redis 服务端单次执行，消除两步之间的竞态
        // （原实现若进程在 INCR 后、EXPIRE 前崩溃，键会永不过期，导致限流窗口失效）。
        // ARGV[1] = 步长 delta，ARGV[2] = 窗口毫秒数（0 表示不设过期）。
        // 首次判定：n == delta ⇔ 自增前值为 0；必须用 tonumber，Lua 里 number ≠ string。
        // 参数必须显式 toString：StringRedisTemplate 的 value 序列化器是 StringRedisSerializer，
        // 直接传 Long 会在 keysAndArgs 阶段抛 ClassCastException（Long cannot be cast to String）。
        return redisTemplate.execute(
            INCR_BY_SCRIPT,
            listOf(key),
            delta.toString(),
            (ttl?.toMillis() ?: 0L).toString(),
        )
    }

    companion object {
        private val INCR_BY_SCRIPT =
            DefaultRedisScript<Long>().apply {
                setScriptText(
                    """
                    local n = redis.call('INCRBY', KEYS[1], ARGV[1])
                    if n == tonumber(ARGV[1]) and ARGV[2] ~= '0' then
                        redis.call('PEXPIRE', KEYS[1], ARGV[2])
                    end
                    return n
                    """.trimIndent(),
                )
                resultType = Long::class.java
            }
    }
}
