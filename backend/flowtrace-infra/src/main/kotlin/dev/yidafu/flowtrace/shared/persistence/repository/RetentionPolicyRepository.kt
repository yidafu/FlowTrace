package dev.yidafu.flowtrace.shared.persistence.repository

import dev.yidafu.flowtrace.shared.model.RetentionScope
import dev.yidafu.flowtrace.shared.persistence.table.RetentionPolicyTable
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.eq
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.upsert
import org.springframework.stereotype.Repository
import java.time.Instant
import java.util.UUID

/**
 * `retention_policy` 的一行（03 §5）。
 *
 * [scope] = USER 的记录覆盖 GLOBAL（生效优先级在 admin 侧解析，本仓储只负责读写）。
 */
data class RetentionPolicyRow(
    val id: UUID,
    val scope: RetentionScope,
    val userId: String?,
    val hotPointDays: Int,
    val segmentHotDays: Int,
    val archiveEnabled: Boolean,
    val archiveKeepDays: Int,
    val updatedAt: Instant,
)

/**
 * `retention_policy` 读写仓储（03 §5，非分区表，DDL 由 exposed-migration 管理）。
 *
 * 策略是「少量行、读多写少」的配置表：归档/删除作业每次运行读一次，
 * admin 通过 `PUT /v1/admin/retention-policies` 写入。写路径按主键 `id` upsert，
 * 调用方负责复用已有 `id`（GLOBAL 用固定 id，USER 记录先在 [findByScope] 里找）。
 */
@Repository
open class RetentionPolicyRepository {
    /** 全部策略，GLOBAL 在前、同范围内按 user_id 排序，便于后台列表稳定分页。 */
    fun list(): List<RetentionPolicyRow> =
        transaction {
            RetentionPolicyTable
                .selectAll()
                .orderBy(
                    RetentionPolicyTable.scopeType to SortOrder.ASC,
                    RetentionPolicyTable.userId to SortOrder.ASC,
                ).map { it.toRetentionPolicyRow() }
        }

    /** 按主键查。 */
    fun findById(id: UUID): RetentionPolicyRow? =
        transaction {
            RetentionPolicyTable
                .selectAll()
                .where { RetentionPolicyTable.id eq id }
                .map { it.toRetentionPolicyRow() }
                .singleOrNull()
        }

    /**
     * 按生效范围查：GLOBAL 传 `userId = null`；USER 记录必传 userId。
     *
     * 表上没有 `(scope_type, user_id)` 唯一约束，理论上可存在多条 USER 记录；
     * 这里取 `updated_at` 最新的一条，让「最后写入者生效」的语义可预期。
     */
    fun findByScope(
        scope: RetentionScope,
        userId: String? = null,
    ): RetentionPolicyRow? =
        transaction {
            require(scope == RetentionScope.USER || userId == null) {
                "GLOBAL 策略不带 user_id: userId=$userId"
            }
            require(scope == RetentionScope.GLOBAL || userId != null) {
                "USER 策略必须带 user_id"
            }
            var condition = RetentionPolicyTable.scopeType eq scope.value
            if (userId != null) {
                condition = condition and (RetentionPolicyTable.userId eq userId)
            }
            RetentionPolicyTable
                .selectAll()
                .where(condition)
                .orderBy(RetentionPolicyTable.updatedAt to SortOrder.DESC)
                .limit(1)
                .map { it.toRetentionPolicyRow() }
                .singleOrNull()
        }

    /** 新建或更新一条策略（按主键 upsert）。 */
    fun upsert(row: RetentionPolicyRow) {
        transaction {
            RetentionPolicyTable.upsert(RetentionPolicyTable.id) {
                it[RetentionPolicyTable.id] = row.id
                it[RetentionPolicyTable.scopeType] = row.scope.value
                it[RetentionPolicyTable.userId] = row.userId
                it[RetentionPolicyTable.hotPointDays] = row.hotPointDays
                it[RetentionPolicyTable.segmentHotDays] = row.segmentHotDays
                it[RetentionPolicyTable.archiveEnabled] = row.archiveEnabled
                it[RetentionPolicyTable.archiveKeepDays] = row.archiveKeepDays
                it[RetentionPolicyTable.updatedAt] = row.updatedAt
            }
        }
    }

    /** 删除一条策略。@return 影响行数 */
    fun deleteById(id: UUID): Int =
        transaction {
            RetentionPolicyTable.deleteWhere { RetentionPolicyTable.id eq id }
        }

    /** 用户删除轨迹时一并清掉该用户的覆盖策略（03 §10 删除矩阵的延伸）。@return 影响行数 */
    fun deleteByUser(userId: String): Int =
        transaction {
            RetentionPolicyTable.deleteWhere { RetentionPolicyTable.userId eq userId }
        }

    private fun ResultRow.toRetentionPolicyRow(): RetentionPolicyRow =
        RetentionPolicyRow(
            id = this[RetentionPolicyTable.id],
            scope = RetentionScope.from(this[RetentionPolicyTable.scopeType]),
            userId = this[RetentionPolicyTable.userId],
            hotPointDays = this[RetentionPolicyTable.hotPointDays],
            segmentHotDays = this[RetentionPolicyTable.segmentHotDays],
            archiveEnabled = this[RetentionPolicyTable.archiveEnabled],
            archiveKeepDays = this[RetentionPolicyTable.archiveKeepDays],
            updatedAt = this[RetentionPolicyTable.updatedAt],
        )
}
