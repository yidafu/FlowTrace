package dev.yidafu.flowtrace.shared.persistence.repository

import dev.yidafu.flowtrace.shared.persistence.table.AuditLogTable
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.eq
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.greaterEq
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.less
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.springframework.stereotype.Repository
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

/** `audit_log` 的一行（03 §5）。[detailJson] 是 JSON 文本（列类型 text，见表定义注释）。 */
data class AuditLogRow(
    val id: UUID,
    val actor: String,
    val action: String,
    val target: String,
    val detailJson: String,
    val createdAt: Instant,
)

/** 审计查询游标：`(created_at, id)` keyset 位置（按时间倒序翻页）。 */
data class AuditLogCursor(
    val createdAt: Instant,
    val id: UUID,
)

/**
 * `audit_log` 写入与查询仓储（03 §5，按月分区，保留 400 天）。
 *
 * 约定：
 * - `actor` 为 userId 或 `system:{job名}`；`action` 取值见 [AuditActions]；
 * - `detail` 列是 JSON 文本（与 geometry 存 WKT 同一「文本序列化 + 应用层处理」约定），
 *   仓储不做序列化，由调用方给 JSON 字符串；
 * - 主键含分区键 `created_at`，所以 `id` 用 **UUIDv7（时间有序）** 生成，
 *   让同一毫秒内的审计记录也有稳定顺序，游标翻页不会跳行/重复；
 * - 审计写入失败不应改变业务结果：调用方按需 try/catch 并打点，不要让它回滚主流程。
 */
@Repository
open class AuditLogRepository {
    /**
     * 追加一条审计记录。
     *
     * @param action 见 [AuditActions]
     * @param detailJson JSON 文本，缺省 `{}`
     * @return 生成的记录 id
     */
    fun append(
        actor: String,
        action: String,
        target: String,
        detailJson: String = "{}",
        createdAt: Instant = Instant.now(),
    ): UUID {
        val id = uuidV7(createdAt)
        transaction {
            AuditLogTable.insert {
                it[AuditLogTable.id] = id
                it[AuditLogTable.actor] = actor
                it[AuditLogTable.action] = action
                it[AuditLogTable.target] = target
                it[AuditLogTable.detail] = detailJson
                it[AuditLogTable.createdAt] = createdAt
            }
        }
        return id
    }

    /**
     * 审计查询（`GET /v1/admin/audit`）：按时间倒序，`(created_at, id)` 游标翻页。
     *
     * 所有过滤条件都可选；时间窗以 `created_at` 收敛，跨月时 PG 按分区裁剪。
     */
    fun list(
        actor: String? = null,
        action: String? = null,
        fromInclusive: Instant? = null,
        toExclusive: Instant? = null,
        limit: Int = DEFAULT_LIMIT,
        after: AuditLogCursor? = null,
    ): List<AuditLogRow> =
        transaction {
            val filters =
                listOfNotNull(
                    actor?.let { AuditLogTable.actor eq it },
                    action?.let { AuditLogTable.action eq it },
                    fromInclusive?.let { AuditLogTable.createdAt greaterEq it },
                    toExclusive?.let { AuditLogTable.createdAt less it },
                    after?.let { cursor ->
                        (AuditLogTable.createdAt less cursor.createdAt) or
                            (
                                (AuditLogTable.createdAt eq cursor.createdAt) and
                                    (AuditLogTable.id less cursor.id)
                            )
                    },
                )
            val base = AuditLogTable.selectAll()
            val filtered = if (filters.isEmpty()) base else base.where(filters.reduce { acc, op -> acc and op })
            filtered
                .orderBy(
                    AuditLogTable.createdAt to SortOrder.DESC,
                    AuditLogTable.id to SortOrder.DESC,
                ).limit(limit)
                .map { it.toAuditLogRow() }
        }

    private fun ResultRow.toAuditLogRow(): AuditLogRow =
        AuditLogRow(
            id = this[AuditLogTable.id],
            actor = this[AuditLogTable.actor],
            action = this[AuditLogTable.action],
            target = this[AuditLogTable.target],
            detailJson = this[AuditLogTable.detail],
            createdAt = this[AuditLogTable.createdAt],
        )

    /**
     * 生成 UUIDv7（RFC 9562 §5.7）：48 位毫秒时间戳 + 版本位 0111 + 12 位随机 + 62 位随机。
     *
     * Java 21 没有内置 UUIDv7；这里只需「时间有序」，不做时钟回拨保护
     * （同毫秒内由随机位保证唯一，游标顺序由 `(created_at, id)` 共同决定）。
     */
    private fun uuidV7(now: Instant): UUID {
        val random = ThreadLocalRandom.current()
        val ms = now.toEpochMilli() and 0xFFFF_FFFF_FFFFL
        val high = (ms shl 16) or 0x7000L or (random.nextLong() and 0x0FFFL)
        val low = (random.nextLong() and 0x3FFF_FFFF_FFFF_FFFFL) or Long.MIN_VALUE
        return UUID(high, low)
    }

    companion object {
        /** 查询默认页大小。 */
        const val DEFAULT_LIMIT: Int = 100
    }
}

/** 审计动作取值（03 §5、07 §5）。新增动作只加常量，不改表结构。 */
object AuditActions {
    const val USER_TRACES_DELETE: String = "USER_TRACES_DELETE"
    const val DEVICE_DISABLE: String = "DEVICE_DISABLE"
    const val POLICY_UPDATE: String = "POLICY_UPDATE"
    const val REPROCESS: String = "REPROCESS"
    const val PARTITION_DROP: String = "PARTITION_DROP"
    const val PARTITION_CREATE: String = "PARTITION_CREATE"
    const val ARCHIVE_RUN: String = "ARCHIVE_RUN"
    const val DLQ_REPLAY: String = "DLQ_REPLAY"
}
