package dev.yidafu.flowtrace.storage.redis

import dev.yidafu.flowtrace.common.cache.WatermarkStore
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component

@Component
class RedisWatermarkStore(
    private val redis: StringRedisTemplate,
) : WatermarkStore {
    override fun maxSeenTs(deviceId: String): Long? {
        // 首次消费时水位 key 不存在，get 返回 null；不能让 null 走到 parseLong
        // （Java 17+ 会抛 NumberFormatException: Cannot parse null string）。
        val tsStr = redis.opsForValue().get(keyOf(deviceId)) ?: return null
        // 存量脏值同样按「无水位」处理，避免单条坏值永久卡住该设备的分区
        return tsStr.toLongOrNull()
    }

    override fun writeIfGreater(
        deviceId: String,
        tsMs: Long,
    ) {
        val oldTsMs = maxSeenTs(deviceId)
        if (oldTsMs == null || oldTsMs < tsMs) {
            redis.opsForValue().set(keyOf(deviceId), tsMs.toString())
        }
    }

    fun keyOf(deviceId: String) = "wm:dev:$deviceId"
}
