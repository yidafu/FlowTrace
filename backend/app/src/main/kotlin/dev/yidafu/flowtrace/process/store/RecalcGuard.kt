package dev.yidafu.flowtrace.process.store

import dev.yidafu.flowtrace.process.config.ProcessorProperties
import dev.yidafu.flowtrace.shared.cache.RecalcLockStore
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * 迟到重算与在线分段的互斥标志（06 §3.6 / 08 §7）。
 *
 * 语义：**每个 userId 一把软锁**。重算按「用户 × 天」重写该用户的全部段，此时若在线链路
 * 继续对同一设备分段，两条写路径会踩同一个 OPEN 窗口（一边 replaceTail 一边 finalizeOpen），
 * 结果既可能丢段也可能重复。所以 `BatchPipeline` 见锁即跳过分段（点仍落库并登记 dirty_day）。
 *
 * 只做「标志」不做「排他」：不阻塞、不排队，在线链路永远走快速失败分支——
 * 轨迹点比段更不可补，绝不能让在线消费线程为重算停下来。
 *
 * **降级方向由本类决定**（08 §7 要求两个方向相反，不能推到存储层）：
 * - [isLocked] 查询失败 → 视为「已加锁」：宁可这批少分一段（会被 dirty_day 重算补上），
 *   也不能在重算写着同一批点时分段，让两条写路径互删；
 * - [tryLock] 加锁失败 → 返回 false：本次重算让路，下一轮扫描再来。
 *
 * 锁 TTL 来自 `flowtrace.processor.recalc-lock-ttl-sec`（默认 300s）：进程崩溃不会留下永久锁。
 */
@Component
class RecalcGuard(
    private val lockStore: RecalcLockStore,
    private val props: ProcessorProperties,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /** 是否有重算正在进行；Redis 不可用时按「已加锁」处理（见类注释的降级方向）。 */
    fun isLocked(userId: String): Boolean =
        try {
            lockStore.isLocked(userId)
        } catch (ex: Exception) {
            log.warn("重算锁查询失败，按已加锁处理（本批跳过分段并入 dirty_day） userId={} cause={}", userId, ex.message)
            true
        }

    /**
     * 尝试加锁（SETNX + TTL）：已持有则返回 false（调用方应让路，不要等）。
     * 存储异常时同样返回 false——加不上锁就不该开始重算。
     */
    fun tryLock(userId: String): Boolean =
        try {
            lockStore.tryLock(userId, ttl)
        } catch (ex: Exception) {
            log.warn("重算锁获取失败，本次重算跳过 userId={} cause={}", userId, ex.message)
            false
        }

    /** 释放锁（重算正常结束或失败时都要调；不存在时静默）。 */
    fun unlock(userId: String) {
        try {
            lockStore.unlock(userId)
        } catch (ex: Exception) {
            // 重算结果已落库，释放失败只是让该用户多等一个 TTL；不能把异常抛给调度线程
            log.warn("重算锁释放失败（等 TTL 自然过期） userId={} cause={}", userId, ex.message)
        }
    }

    /**
     * 锁 TTL 来自 `flowtrace.processor.recalc-lock-ttl-sec`（默认 300s）：进程崩溃不会留下永久锁。
     *
     * 键名（`recalc:u:{userId}`，06 §2.4）由 `RecalcLockStore` 的实现决定唯一事实源，
     * 这里不再重复一份字符串拼接——两处格式一旦分叉，隐私删除路径就会清错键。
     */
    private val ttl: Duration
        get() = Duration.ofSeconds(props.recalcLockTtlSec)
}
