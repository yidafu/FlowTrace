package dev.yidafu.flowtrace.shared.persistence.repository

import dev.yidafu.flowtrace.shared.model.DirtyDayReason
import dev.yidafu.flowtrace.shared.model.DirtyDayStatus
import dev.yidafu.flowtrace.shared.persistence.table.DirtyDayTable
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.eq
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.vendors.ForUpdateOption
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.insertIgnore
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import org.springframework.stereotype.Repository
import java.time.Instant
import java.time.LocalDate

/** `dirty_day` 的一行（03 §5，补录重算队列）。 */
data class DirtyDayRow(
    val userId: String,
    val day: LocalDate,
    val reason: DirtyDayReason,
    val attempts: Short,
    val status: DirtyDayStatus,
    val firstSeenAt: Instant,
)

/**
 * `dirty_day` 读写仓储（03 §5 / 决策 D13，非分区表）。
 *
 * 队列语义：processor 发现迟到批入队（[enqueue]），重算线程抢占（[claimBatch]）后
 * 读点重算段，再置 DONE / 失败重排（[markDone] / [markFailed]）。
 *
 * 抢占必须 `FOR UPDATE SKIP LOCKED`：多实例、多线程并发轮询时跳过别人锁住的行，
 * 否则同一 `(user, day)` 会被重复重算——重算本身幂等，但白烧 CPU（08 §6.4）。
 * Exposed 1.0.0-beta-4 不支持在 `deleteWhere`/`update` 上加锁选项，
 * 但 [org.jetbrains.exposed.v1.jdbc.Query.forUpdate] 支持
 * `ForUpdateOption.PostgreSQL.ForUpdate(MODE.SKIP_LOCKED)`，因此抢占走 SELECT + 加锁。
 */
@Repository
open class DirtyDayRepository {
    /**
     * 入队（幂等）：`ON CONFLICT (user_id, day) DO NOTHING`。
     *
     * 同一 (user, day) 被多次判定迟到只留一行，不重置 `attempts`（重试次数要累计到人工介入）；
     * `reason` 取首次入队原因，MANUAL 触发的重算若已存在于队列则沿用原记录。
     *
     * @return 实际入队天数（已存在的不计）
     */
    fun enqueue(
        userId: String,
        days: Collection<LocalDate>,
        reason: DirtyDayReason = DirtyDayReason.LATE_DATA,
        now: Instant = Instant.now(),
    ): Int {
        if (days.isEmpty()) return 0
        return transaction {
            days.distinct().sumOf { day ->
                DirtyDayTable
                    .insertIgnore {
                        it[DirtyDayTable.userId] = userId
                        it[DirtyDayTable.day] = day
                        it[DirtyDayTable.reason] = reason.value
                        it[DirtyDayTable.attempts] = 0
                        it[DirtyDayTable.status] = DirtyDayStatus.PENDING.value
                        it[DirtyDayTable.firstSeenAt] = now
                    }.insertedCount
            }
        }
    }

    /**
     * 抢占一批待重算任务（08 §6.4），返回时状态已置 [DirtyDayStatus.RUNNING]。
     *
     * 行锁 + `SKIP LOCKED` 保证同一行不会被两个重算线程同时拿到；随后**在同一事务内**
     * 把状态改成 RUNNING，让锁释放后其他轮询者也不会再挑到它（崩溃时靠人工/巡检把
     * 卡在 RUNNING 的行改回 PENDING，表上没有 `updated_at` 无法自动判超时）。
     *
     * @param limit 单次抢占上限，默认 10（08 §6.4）
     */
    fun claimBatch(limit: Int = DEFAULT_CLAIM_LIMIT): List<DirtyDayRow> {
        if (limit <= 0) return emptyList()
        return transaction {
            val claimed =
                DirtyDayTable
                    .selectAll()
                    .where { DirtyDayTable.status eq DirtyDayStatus.PENDING.value }
                    .orderBy(DirtyDayTable.firstSeenAt to SortOrder.ASC)
                    .limit(limit)
                    .forUpdate(ForUpdateOption.PostgreSQL.ForUpdate(ForUpdateOption.PostgreSQL.MODE.SKIP_LOCKED))
                    .map { it.toDirtyDayRow() }
            claimed.forEach { row ->
                DirtyDayTable.update({
                    (DirtyDayTable.userId eq row.userId) and (DirtyDayTable.day eq row.day)
                }) {
                    it[DirtyDayTable.status] = DirtyDayStatus.RUNNING.value
                }
            }
            claimed.map { it.copy(status = DirtyDayStatus.RUNNING) }
        }
    }

    /** 标记重算完成。@return 影响行数（0 表示任务已被清理） */
    fun markDone(
        userId: String,
        day: LocalDate,
    ): Int =
        transaction {
            DirtyDayTable.update({
                (DirtyDayTable.userId eq userId) and (DirtyDayTable.day eq day)
            }) {
                it[DirtyDayTable.status] = DirtyDayStatus.DONE.value
            }
        }

    /**
     * 标记重算失败：`attempts + 1`；未达 [maxAttempts] 回到 PENDING 重排队，达上限置 FAILED。
     *
     * @return 更新后的状态；任务不存在返回 null
     */
    fun markFailed(
        userId: String,
        day: LocalDate,
        maxAttempts: Short = DEFAULT_MAX_ATTEMPTS,
    ): DirtyDayStatus? =
        transaction {
            val current =
                DirtyDayTable
                    .selectAll()
                    .where { (DirtyDayTable.userId eq userId) and (DirtyDayTable.day eq day) }
                    .map { it.toDirtyDayRow() }
                    .singleOrNull()
                    ?: return@transaction null
            val attempts = (current.attempts + 1).toShort()
            val next = if (attempts >= maxAttempts) DirtyDayStatus.FAILED else DirtyDayStatus.PENDING
            DirtyDayTable.update({
                (DirtyDayTable.userId eq userId) and (DirtyDayTable.day eq day)
            }) {
                it[DirtyDayTable.attempts] = attempts
                it[DirtyDayTable.status] = next.value
            }
            next
        }

    /**
     * 放回队列且**不增加 attempts**：抢占后因「在线链路正在写同一用户」而主动让路时的回退。
     *
     * 与 [markFailed] 的区别是语义：让路不是失败，不能消耗重试次数（否则连续几轮抢不到锁就把任务
     * 打成 FAILED 等人工介入）。同时避免任务卡在 RUNNING——`claimBatch` 只挑 PENDING，
     * 表上没有 `updated_at`，卡住的行只能靠人工捞。
     *
     * @return 影响行数
     */
    fun requeue(
        userId: String,
        day: LocalDate,
    ): Int =
        transaction {
            DirtyDayTable.update({
                (DirtyDayTable.userId eq userId) and (DirtyDayTable.day eq day)
            }) {
                it[DirtyDayTable.status] = DirtyDayStatus.PENDING.value
            }
        }

    /** 用户当前的全部重算任务（admin 排查「为什么某天数据没更新」用）。 */
    fun listByUser(userId: String): List<DirtyDayRow> =
        transaction {
            DirtyDayTable
                .selectAll()
                .where { DirtyDayTable.userId eq userId }
                .orderBy(DirtyDayTable.day to SortOrder.DESC)
                .map { it.toDirtyDayRow() }
        }

    /**
     * 删除用户的重算任务（03 §10 删除矩阵：user 数据清空后队列条目也必须清）。
     *
     * @return 删除行数
     */
    fun deleteByUser(userId: String): Int =
        transaction {
            DirtyDayTable.deleteWhere { DirtyDayTable.userId eq userId }
        }

    private fun ResultRow.toDirtyDayRow(): DirtyDayRow =
        DirtyDayRow(
            userId = this[DirtyDayTable.userId],
            day = this[DirtyDayTable.day],
            reason = DirtyDayReason.from(this[DirtyDayTable.reason]),
            attempts = this[DirtyDayTable.attempts],
            status = DirtyDayStatus.from(this[DirtyDayTable.status]),
            firstSeenAt = this[DirtyDayTable.firstSeenAt],
        )

    companion object {
        /** 单次抢占上限（08 §6.4）。 */
        const val DEFAULT_CLAIM_LIMIT: Int = 10

        /** 失败重试上限：达到后置 FAILED 等人工介入（06 §3.6）。 */
        const val DEFAULT_MAX_ATTEMPTS: Short = 3
    }
}
