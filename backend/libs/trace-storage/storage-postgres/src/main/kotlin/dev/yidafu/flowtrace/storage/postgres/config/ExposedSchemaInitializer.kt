package dev.yidafu.flowtrace.storage.postgres.config

import dev.yidafu.flowtrace.storage.postgres.table.DeviceTable
import dev.yidafu.flowtrace.storage.postgres.table.DirtyDayTable
import dev.yidafu.flowtrace.storage.postgres.table.RetentionPolicyTable
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.stereotype.Component

/**
 * 用 Exposed DSL（SchemaUtils.create）按 Table 对象建表，替代手写/生成 SQL 脚本。
 *
 * 默认关闭（ddl-auto=none）：app 启动测试无真实 PostgreSQL，开启会令启动期连库失败。
 * 置 flowtrace.postgres.ddl-auto=create 后，在应用启动后补建缺失的表；
 * SchemaUtils.create 幂等——只建不存在的表，不改动已有表结构。
 * 已有表的结构演进仍按 D16 走 exposed-migration，本初始化器只负责首次建表。
 *
 * 仅纳入非分区表（device / retention_policy / dirty_day）。分区表（trace_point /
 * trace_segment / trace_batch / audit_log）的 Table 对象只是列映射事实源，其主表
 * CREATE TABLE ... PARTITION BY 与分区子表由 PartitionManager（03 §7）运行时 DDL 创建，
 * 若交给 SchemaUtils 会建成无分区的普通表，破坏分区设计，故不在此处建。
 */
@Component
class ExposedSchemaInitializer(
    @Value($$"${flowtrace.postgres.ddl-auto:none}")
    private val ddlAuto: String,
) : CommandLineRunner {
    override fun run(vararg args: String) {
        if (!ddlAuto.equals("create", ignoreCase = true)) return
        transaction {
            SchemaUtils.create(DeviceTable, RetentionPolicyTable, DirtyDayTable)
        }
    }
}
