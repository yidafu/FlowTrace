package dev.yidafu.flowtrace.shared.cache

/**
 * 上报批次幂等闸门（06 §2.2 第 7 步，04 §1「POST /v1/traces/batch 幂等（batchId）」）。
 *
 * 语义是**占用（SETNX）而不是查询**：先占坑再干活，命中说明这个 batchId 已经被受理过。
 * 与「先查后写」的区别在于并发下也只有一个请求能通过——SDK 弱网重试、双通道同时触发
 * 都可能让同一批在毫秒级内到达两次。
 *
 * 生命周期与调用方约定（D19）：
 * 1. 解码/校验/限流都通过后 [tryAcquire]；
 * 2. 后续步骤失败（尤其 Kafka 写失败）**必须** [release] 回滚占位——
 *    否则「返回 5xx + 已占位」会让 SDK 的重试被当成重复批静默丢弃（数据永久丢失）；
 * 3. 成功受理后**不要**释放，让 TTL 自然过期（默认 7d，覆盖端侧最长补传窗口）。
 */
interface IdempotencyGate {
    /**
     * 尝试占用 [key]（如 batchId）。
     *
     * @return true = 首次占用，继续走后续链路；false = 重复批，直接返回 `deduplicated: true`
     */
    fun tryAcquire(key: String): Boolean

    /** 回滚占用（仅用于「已占用但后续步骤失败」的路径）。 */
    fun release(key: String)
}
