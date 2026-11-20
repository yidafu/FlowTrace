package dev.yidafu.flowtrace.realtime

import dev.yidafu.flowtrace.shared.cache.FlowtraceCache
import dev.yidafu.flowtrace.shared.cache.get
import org.springframework.stereotype.Component

/**
 * 最新位置存储：经 [FlowtraceCache] 抽象读写 Redis，不感知底层实现。
 *
 * 不设 TTL：stale 语义（04 §3，updatedAt 距当前 >5min，STALE_MS=300000）依赖值内
 * updatedAtMs 判定，条目过期会丢失 stale=true 返回能力；键的清理走隐私删除作业
 * （DELETE /v1/users/{userId}/traces 覆盖所有存储，含 Redis）。
 */
@Component
class LatestLocationStore(
    private val cache: FlowtraceCache,
) {
    /** 写入/覆盖用户最新位置（Kafka 消费侧调用）。 */
    fun save(record: LatestLocationRecord) {
        cache.put(key(record.userId), record)
    }

    /** 读取用户最新位置；无数据返回 null（控制器据此回 404 NOT_FOUND）。 */
    fun find(userId: Long): LatestLocationRecord? = cache.get<LatestLocationRecord>(key(userId))

    private fun key(userId: Long): String = "loc:$userId"
}

/** 最新位置值对象（字段对应 04 §3 /location 响应；不含派生字段 stale）。 */
data class LatestLocationRecord(
    val userId: Long,
    val deviceId: String,
    val tsMs: Long,
    val lat: Double,
    val lng: Double,
    val accuracyCm: Long,
    val speedMps: Double? = null,
    val bearingDeg: Double? = null,
    val updatedAtMs: Long,
)
