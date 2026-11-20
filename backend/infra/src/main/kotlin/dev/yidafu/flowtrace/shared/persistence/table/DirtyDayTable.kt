package dev.yidafu.flowtrace.shared.persistence.table

import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.date
import org.jetbrains.exposed.v1.javatime.timestamp

/**
 * dirty_day 表（03 §5，补录重算队列，决策 D13）。
 *
 * 非分区表，纳入 SchemaUtils.create 首次建表。
 * processor 写入，admin 重算任务消费（06 §4.6）。
 * reason：1 LATE_DATA / 2 MANUAL；status：1 PENDING / 2 RUNNING / 3 DONE / 4 FAILED。
 */
object DirtyDayTable : Table("dirty_day") {
    val userId = varchar("user_id", 64)
    val day = date("day")
    val reason = short("reason")
    val attempts = short("attempts").default(0)
    val status = short("status")
    val firstSeenAt = timestamp("first_seen_at")

    override val primaryKey = PrimaryKey(userId, day)
}
