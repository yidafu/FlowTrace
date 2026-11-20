package dev.yidafu.flowtrace.shared.persistence.table

import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

/**
 * retention_policy 表（03 §5，保留策略）。
 *
 * 非分区表，纳入 SchemaUtils.create 首次建表。
 * scope_type：1 GLOBAL / 2 USER；USER 记录覆盖 GLOBAL。
 */
object RetentionPolicyTable : Table("retention_policy") {
    val id = uuid("id")
    val scopeType = short("scope_type")
    val userId = varchar("user_id", 64).nullable()
    val hotPointDays = integer("hot_point_days").default(30)
    val segmentHotDays = integer("segment_hot_days").default(180)
    val archiveEnabled = bool("archive_enabled").default(true)
    val archiveKeepDays = integer("archive_keep_days").default(365)
    val updatedAt = timestamp("updated_at")

    override val primaryKey = PrimaryKey(id)
}
