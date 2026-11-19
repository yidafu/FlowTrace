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
 * 启动期补建缺失的表，替代手写/生成 SQL 脚本。
 *
 * 默认关闭（ddl-auto=none）：app 启动测试无真实 PostgreSQL，开启会令启动期连库失败。
 * 置 flowtrace.postgres.ddl-auto=create 后执行两类 DDL：
 * 1. 非分区表（device / retention_policy / dirty_day）走 `SchemaUtils.create`；
 * 2. 分区表（trace_point / trace_segment / trace_batch / audit_log）走 [PartitionedSchema]——
 *    Exposed 建不出 `PARTITION BY RANGE`，交给 `SchemaUtils` 会建成无分区普通表，
 *    后续拿不到 `DROP PARTITION` 过期删除与分区裁剪。
 *
 * 两者都幂等：只建缺失的表/分区，不改动已有结构；已有表的结构演进仍按 D16 走
 * exposed-migration，本初始化器只负责首次建表。
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
            PartitionedSchema.createAll()
        }
    }
}
