package dev.yidafu.flowtrace.shared.cache

import java.time.Duration

/**
 * 迟到重算互斥锁（06 §3.6 `recalc:u:{userId}`，08 §7）。
 *
 * 单独成型而不是复用 [FlowtraceCache]：这里要的是 **SETNX + TTL 的原子语义**，
 * 而 `FlowtraceCache.put` 是无条件覆盖（两个重算线程会同时「加锁成功」，
 * 随后并发重写同一用户的段）。08 §7 明确要求新增专用组件，不要硬塞进通用缓存。
 *
 * 实现方（Redis）在异常时**直接抛出**：是否降级由调用方（processor 的 `RecalcGuard`）决定——
 * 在线链路要求「锁不可用按已加锁处理」，重算任务要求「加不上锁就跳过」，
 * 两种降级方向相反，不能由存储层替业务拍板。
 */
interface RecalcLockStore {
    /**
     * 尝试加锁（`SET key value NX PX ttl`）。
     *
     * @return true = 本次抢到锁；false = 已被他人持有（调用方应让路，不要等待）
     */
    fun tryLock(
        userId: String,
        ttl: Duration,
    ): Boolean

    /** 是否已被持有（在线链路每批调用一次，只读）。 */
    fun isLocked(userId: String): Boolean

    /**
     * 批量查询已被持有的锁（处理合批链路用，08 §4）：返回已加锁的 userId 集合。
     *
     * 实现必须用**一次 MGET 往返**覆盖全部入参；异常语义与 [isLocked] 一致——**直接抛出**，
     * 降级方向（在线链路按全部已加锁处理）由调用方（processor 的 `RecalcGuard`）决定。
     * 空入参直接返回空集，不打 Redis。
     */
    fun filterLocked(userIds: Collection<String>): Set<String>

    /** 释放锁；不存在时静默（重算正常结束与异常退出路径都要调）。 */
    fun unlock(userId: String)
}
