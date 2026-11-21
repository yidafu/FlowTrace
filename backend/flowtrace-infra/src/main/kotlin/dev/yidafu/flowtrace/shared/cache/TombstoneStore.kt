package dev.yidafu.flowtrace.shared.cache

import java.time.Duration

interface TombstoneStore {
    fun has(userId: String): Boolean

    /**
     * 批量查询墓碑（处理合批链路用，08 §4「poll 攒 N 批合并写」）：返回命中墓碑的 userId 集合。
     *
     * 实现必须用**一次 MGET 往返**覆盖全部入参——逐个 [has] 会把合批摊薄 Redis 往返的收益吐回去。
     * 空入参直接返回空集，不打 Redis。
     */
    fun filterTombstoned(userIds: Collection<String>): Set<String>

    fun mark(
        userId: String,
        jobId: String,
        ttl: Duration,
    )
}
