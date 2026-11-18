package dev.yidafu.flowtrace.storage.postgres.table

import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

/**
 * device 表（03 §5，admin 域非分区表，DDL 由 exposed-migration 管理）。
 *
 * status 存 int2（DeviceStatus.value）；user_id 全链路字符串，varchar(64)，不做数值化。
 */
object DeviceTable : Table("device") {
    val deviceId = varchar("device_id", 64)
    val userId = varchar("user_id", 64)
    val platform = varchar("platform", 32).nullable()
    val appVersion = varchar("app_version", 64).nullable()
    val sdkVersion = varchar("sdk_version", 64).nullable()
    val status = short("status")
    val lastSeenAt = timestamp("last_seen_at").nullable()
    val createdAt = timestamp("created_at")

    override val primaryKey = PrimaryKey(deviceId)

    init {
        index(customIndexName = "device_user_id_idx", false, userId)
    }
}
