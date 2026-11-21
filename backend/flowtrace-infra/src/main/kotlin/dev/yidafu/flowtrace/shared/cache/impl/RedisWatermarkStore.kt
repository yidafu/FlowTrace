package dev.yidafu.flowtrace.shared.cache.impl

import dev.yidafu.flowtrace.shared.cache.WatermarkStore
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Component

/**
 * 设备水位 `wm:dev:{deviceId}`（06 §3.2 第 3 步，08 §7）。
 *
 * 水位语义：**该设备已见过的最大点 ts**。它只增不减，用来判「本批是否迟到」——
 * 批内最小 ts 早于「水位 − 10min」即迟到（跳过分段、登记 dirty_day 重算）。
 *
 * [writeIfGreater] 必须是**原子**的比较写：
 * - 非原子实现（GET 后再 SET）在并发下会把水位写回退：两个 consumer 短暂同时持有同一分区
 *   （重平衡瞬间）时，先读到旧值的一方可能后写入，水位倒退 → 后续正常批被误判为迟到；
 * - 因此用 Lua 把「读 + 比较 + 写」压在 Redis 单线程里一次执行（08 §7 明确要求）。
 */
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

    /** MGET 一次覆盖全部设备；解析口径与 [maxSeenTs] 一致（null / 脏值按「无水位」跳过）。 */
    override fun maxSeenTsBatch(deviceIds: Collection<String>): Map<String, Long> {
        if (deviceIds.isEmpty()) return emptyMap()
        val distinct = deviceIds.distinct()
        val values: List<String?> = redis.opsForValue().multiGet(distinct.map { keyOf(it) })
        val result = LinkedHashMap<String, Long>()
        distinct.forEachIndexed { i, id ->
            values[i]?.toLongOrNull()?.let { result[id] = it }
        }
        return result
    }

    override fun writeIfGreater(
        deviceId: String,
        tsMs: Long,
    ) {
        // 参数必须显式 toString：StringRedisTemplate 的 value 序列化器是 StringRedisSerializer，
        // 直接传 Long 会在 keysAndArgs 阶段抛 ClassCastException
        redis.execute(WRITE_IF_GREATER_SCRIPT, listOf(keyOf(deviceId)), tsMs.toString())
    }

    /**
     * 多键一次 EVAL（[WatermarkStore.writeIfGreaterBatch] 契约）：KEYS[i] 与 ARGV[i] 一一配对，
     * 整段脚本在 Redis 单线程里原子执行，比较写语义与 [WRITE_IF_GREATER_SCRIPT] 逐键完全一致。
     *
     * 多键脚本要求所有键落在同一 slot：当前是单体 Redis（deploy/ 的 compose 口径）天然满足；
     * 若将来上 Redis Cluster，需按 hash-tag 归并后再分片调用。
     */
    override fun writeIfGreaterBatch(entries: Map<String, Long>) {
        if (entries.isEmpty()) return
        val keys = ArrayList<String>(entries.size)
        val args = ArrayList<String>(entries.size)
        // 参数必须显式 toString（同 [writeIfGreater] 的序列化器口径）
        entries.forEach { (device, ts) ->
            keys += keyOf(device)
            args += ts.toString()
        }
        redis.execute(WRITE_IF_GREATER_BATCH_SCRIPT, keys, *args.toTypedArray())
    }

    fun keyOf(deviceId: String) = "wm:dev:$deviceId"

    private companion object {
        /**
         * `GET` → 比较 → `SET`，全部在 Redis 端完成。
         *
         * `tonumber` 是必需的：Redis 里存的是字符串，Lua 的 `>` 对字符串做字典序比较，
         * 会得出 "9" > "10" 这种错误结论。存量脏值（非数字）按「无水位」覆盖，
         * 与 [maxSeenTs] 的容错口径保持一致。
         */
        val WRITE_IF_GREATER_SCRIPT =
            DefaultRedisScript<Long>().apply {
                setScriptText(
                    """
                    local v = redis.call('GET', KEYS[1])
                    local incoming = tonumber(ARGV[1])
                    if (not v) or tonumber(v) == nil or incoming > tonumber(v) then
                        redis.call('SET', KEYS[1], ARGV[1])
                        return 1
                    end
                    return 0
                    """.trimIndent(),
                )
                resultType = Long::class.java
            }

        /**
         * [WRITE_IF_GREATER_SCRIPT] 的多键版：循环内逐键「GET → 比较 → SET」，
         * `tonumber` 的必要性同上（Redis 存字符串，Lua 对字符串做字典序比较）。
         * 返回实际推进的键数（诊断用，调用方不依赖）。
         */
        val WRITE_IF_GREATER_BATCH_SCRIPT =
            DefaultRedisScript<Long>().apply {
                setScriptText(
                    """
                    local written = 0
                    for i = 1, #KEYS do
                      local v = redis.call('GET', KEYS[i])
                      local incoming = tonumber(ARGV[i])
                      if (not v) or tonumber(v) == nil or incoming > tonumber(v) then
                        redis.call('SET', KEYS[i], ARGV[i])
                        written = written + 1
                      end
                    end
                    return written
                    """.trimIndent(),
                )
                resultType = Long::class.java
            }
    }
}
