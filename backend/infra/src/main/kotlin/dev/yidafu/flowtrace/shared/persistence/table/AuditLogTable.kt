package dev.yidafu.flowtrace.shared.persistence.table

import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

/**
 * audit_log 表（03 §5，审计日志）。
 *
 * 按月 RANGE 分区（created_at），是分区主表的列映射事实源：
 * 主表 DDL 与按月子表由 PartitionManager（03 §7）创建，不纳入 SchemaUtils.create。
 * 主键含分区键；id 为 UUIDv7（时间有序）。
 *
 * actor：userId 或 "system:job名"；
 * action：USER_TRACES_DELETE / DEVICE_DISABLE / POLICY_UPDATE / REPROCESS /
 *         PARTITION_DROP / ARCHIVE_RUN / DLQ_REPLAY。保留 400 天。
 * detail 文档定义为 jsonb，此处用 text 列存 JSON 字符串（与 geometry 存 WKT 同样的
 * "文本序列化 + 应用层处理"约定），避免引入 exposed-json 的序列化器依赖；
 * 若后续需要在库内按 detail 字段检索，再切 jsonb 列类型。
 */
object AuditLogTable : Table("audit_log") {
    val id = uuid("id")
    val actor = varchar("actor", 128)
    val action = varchar("action", 64)
    val target = varchar("target", 128)
    val detail = text("detail")
    val createdAt = timestamp("created_at")

    override val primaryKey = PrimaryKey(createdAt, id)
}
