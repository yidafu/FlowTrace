package dev.yidafu.flowtrace.shared.cache

import java.time.Duration

/**
 * 缓存操作抽象：隔离底层缓存实现（当前为 Redis，见 libs/trace-storage/storage-redis；
 * 未来如需可替换为本地缓存/多级缓存），services 只依赖本接口，不见 Redis 类型。
 *
 * key 约定：`{域}:{业务键}`（如 `loc:10001`、`rl:pc:dev-abc`、`cfg:upint:dev-abc`），
 * 域前缀由使用方定义，保持全局可读、无冲突。
 */
interface FlowtraceCache {
    fun has(key: String): Boolean

    /**
     * 读取并反序列化为 [type]；键不存在或已过期返回 null。
     * 底层以 JSON 文本存储，值需可被 Jackson 序列化/反序列化。
     */
    fun <T : Any> get(
        key: String,
        type: Class<T>,
    ): T?

    /**
     * 写入/覆盖缓存。[ttl] 为 null 表示不过期
     * （如最新位置——stale 语义依赖值内 updatedAtMs 判定，不能靠条目过期表达）。
     */
    fun put(
        key: String,
        value: Any,
        ttl: Duration? = null,
    )

    /** 删除指定键（不存在时静默）。隐私删除（04 §4）必须覆盖相关缓存键。 */
    fun evict(key: String)

    /**
     * 计数器按步长 [delta] 自增（限流窗口等场景）：返回自增后的值。
     * 首次自增（自增前值为 0）时设置 [ttl] 窗口过期；由 Redis 实现为 Lua 脚本，
     * INCRBY 与 PEXPIRE 在服务端一次执行，严格原子、无窗口边界计数误差。
     * [delta] 必须为正数——增量 <= 0 会让「首次」判定失效、窗口无法建立。
     * [ttl] 为 null 时只自增、不设过期（调用方需自行保证键的生命周期）。
     */
    fun incrementBy(
        key: String,
        delta: Long,
        ttl: Duration? = null,
    ): Long

    /** 固定 +1 自增（批数限流等场景），等价于 [incrementBy] 的 delta=1 特例。 */
    fun increment(
        key: String,
        ttl: Duration? = null,
    ): Long = incrementBy(key, 1L, ttl)
}

/** 类型安全的便捷读取：`cache.get<Foo>(key)`。 */
inline fun <reified T : Any> FlowtraceCache.get(key: String): T? = get(key, T::class.java)
