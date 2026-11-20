package dev.yidafu.flowtrace.shared.persistence.repository

import dev.yidafu.flowtrace.shared.model.Channel
import dev.yidafu.flowtrace.shared.model.TraceBatchStatus
import dev.yidafu.flowtrace.shared.persistence.table.TraceBatchTable
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.eq
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.greater
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.inList
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.insertIgnore
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import org.springframework.stereotype.Repository
import java.time.Instant
import java.util.UUID

/**
 * `trace_batch` 的一行（03 §4）。
 *
 * 与 [dev.yidafu.flowtrace.shared.model.TracePoint] 不同，批次没有独立的领域对象：
 * 它只服务于幂等兜底、审计与对账，字段即列，直接按行投影。
 */
data class TraceBatchRow(
    val batchId: UUID,
    val userId: String,
    val deviceId: String,
    val channel: Channel,
    val pointCount: Int,
    val firstTs: Instant,
    val lastTs: Instant,
    val clientTs: Instant,
    val receivedAt: Instant,
    val sdkVersion: String,
    val platform: String,
    val status: TraceBatchStatus,
    val errorCode: String? = null,
    val processedAt: Instant? = null,
)

/** 失败批扫描游标：`(received_at, batch_id)` keyset 位置。 */
data class TraceBatchCursor(
    val receivedAt: Instant,
    val batchId: UUID,
)

/** 一次台账状态流转；[applyStatusBatch] 会把 `(status, errorCode)` 相同的合成一条 UPDATE。 */
data class BatchStatusChange(
    val batchId: UUID,
    val status: TraceBatchStatus,
    val errorCode: String? = null,
)

/**
 * `trace_batch` 读写仓储（03 §4，按天分区）。
 *
 * 定位：**审计与兜底表，不在热路径**。
 * - 热路径幂等走 Redis SETNX（06 §3.2 第 7 步），本表唯一索引只在 Redis 丢失后重放时兜底；
 * - 状态更新按 `batch_id` 更新，会扫全部分区（唯一索引落在各子表上），
 *   调用方必须走异步缓冲（08 §6.3 `BatchStatusUpdater`），不要在处理主线程直接调；
 * - `UPDATE ... WHERE batch_id = ?` 影响 0 行**不是错误**（upload 异步写批记录可能滞后/丢失）。
 */
@Repository
open class TraceBatchRepository {
    /**
     * 落批次记录（upload 第 9 步异步写入，06 §2.2）。
     *
     * `ON CONFLICT DO NOTHING`：唯一索引冲突 = Redis 幂等 key 丢失后的同批重放，
     * 忽略即可（重复批不应覆盖首次记录的状态）。
     *
     * @return true 表示本次真的插入了新行；false 表示已存在同 batchId 的记录
     */
    fun insertIfAbsent(row: TraceBatchRow): Boolean =
        transaction {
            TraceBatchTable
                .insertIgnore {
                    it[TraceBatchTable.receivedAt] = row.receivedAt
                    it[TraceBatchTable.batchId] = row.batchId
                    it[TraceBatchTable.userId] = row.userId
                    it[TraceBatchTable.deviceId] = row.deviceId
                    it[TraceBatchTable.channel] = row.channel.value
                    it[TraceBatchTable.pointCount] = row.pointCount
                    it[TraceBatchTable.firstTs] = row.firstTs
                    it[TraceBatchTable.lastTs] = row.lastTs
                    it[TraceBatchTable.clientTs] = row.clientTs
                    it[TraceBatchTable.sdkVersion] = row.sdkVersion
                    it[TraceBatchTable.platform] = row.platform
                    it[TraceBatchTable.status] = row.status.value
                    it[TraceBatchTable.errorCode] = row.errorCode
                    it[TraceBatchTable.processedAt] = row.processedAt
                }.insertedCount > 0
        }

    /**
     * 标记处理成功（processor 第 8 步，异步缓冲调用）。@return 影响行数
     *
     * 单条入口保留给运维/补数脚本；热路径请用 [applyStatusBatch]，逐条 UPDATE 会把提交次数
     * 拉高到与批数同阶（实测每处理批 ~10 次提交，2026-11-20 容量报告 §6）。
     */
    fun markProcessed(
        batchId: UUID,
        at: Instant = Instant.now(),
    ): Int = applyStatusBatch(listOf(BatchStatusChange(batchId, TraceBatchStatus.PROCESSED)), at)

    /** 标记确定性失败（含墓碑丢弃）。@return 影响行数 */
    fun markFailed(
        batchId: UUID,
        errorCode: String,
        at: Instant = Instant.now(),
    ): Int = applyStatusBatch(listOf(BatchStatusChange(batchId, TraceBatchStatus.FAILED, errorCode)), at)

    /** 标记进入死信（06 §3.7）。@return 影响行数 */
    fun markDlq(
        batchId: UUID,
        errorCode: String,
        at: Instant = Instant.now(),
    ): Int = applyStatusBatch(listOf(BatchStatusChange(batchId, TraceBatchStatus.DLQ, errorCode)), at)

    /**
     * **批量**回写台账状态：一次事务、按 `(status, errorCode)` 分组合成若干条
     * `UPDATE ... WHERE batch_id IN (...)`。
     *
     * 为什么按状态分组而不是逐条：`PROCESSED` 占绝对多数，一批 100 条通常只合成 1 条 UPDATE
     * （失败/死信才多出 1–2 条）。收益有两层——语句数 100 → 1，提交数 100 → 1，
     * 后者才是瓶颈（每次 commit 都要 WAL 刷盘）。
     *
     * 注意 `WHERE batch_id IN (...)` 仍会扫全部子分区（分区键是 `received_at`，这里拿不到），
     * 只是把 100 次索引探测压进一条语句；影响 0 行不是错误（upload 台账可能滞后或被丢）。
     *
     * @return 影响行数合计
     */
    open fun applyStatusBatch(
        changes: List<BatchStatusChange>,
        at: Instant = Instant.now(),
    ): Int {
        if (changes.isEmpty()) return 0
        return transaction {
            changes
                .groupBy { it.status to it.errorCode }
                .entries
                .sumOf { (key, group) ->
                    val (status, code) = key
                    TraceBatchTable.update({ TraceBatchTable.batchId inList group.map { it.batchId } }) {
                        it[TraceBatchTable.status] = status.value
                        it[TraceBatchTable.errorCode] = code
                        it[processedAt] = at
                    }
                }
        }
    }

    /** 按幂等键查批次（对账/运维排查）。 */
    fun findByBatchId(batchId: UUID): TraceBatchRow? =
        transaction {
            TraceBatchTable
                .selectAll()
                .where { TraceBatchTable.batchId eq batchId }
                .map { it.toTraceBatchRow() }
                .singleOrNull()
        }

    /**
     * 失败批扫描（`GET /v1/admin/dlq/batches` 与重处理入口）。
     *
     * 走部分索引 `trace_batch_failed_idx`（`status IN (3, 4)`），按接收时间升序
     * 保证先失败的批先被处理；跨分区按 `(received_at, batch_id)` 游标翻页。
     */
    fun listFailed(
        limit: Int = DEFAULT_LIMIT,
        after: TraceBatchCursor? = null,
    ): List<TraceBatchRow> =
        transaction {
            var condition: Op<Boolean> =
                TraceBatchTable.status inList listOf(TraceBatchStatus.FAILED.value, TraceBatchStatus.DLQ.value)
            if (after != null) {
                condition = condition and (
                    (TraceBatchTable.receivedAt greater after.receivedAt) or
                        (
                            (TraceBatchTable.receivedAt eq after.receivedAt) and
                                (TraceBatchTable.batchId greater after.batchId)
                        )
                )
            }
            TraceBatchTable
                .selectAll()
                .where(condition)
                .orderBy(
                    TraceBatchTable.receivedAt to SortOrder.ASC,
                    TraceBatchTable.batchId to SortOrder.ASC,
                ).limit(limit)
                .map { it.toTraceBatchRow() }
        }

    /**
     * 删除用户批次记录（03 §10 删除矩阵）。
     *
     * @return 删除行数
     */
    fun deleteByUser(userId: String): Int =
        transaction {
            TraceBatchTable.deleteWhere { TraceBatchTable.userId eq userId }
        }

    private fun ResultRow.toTraceBatchRow(): TraceBatchRow =
        TraceBatchRow(
            batchId = this[TraceBatchTable.batchId],
            userId = this[TraceBatchTable.userId],
            deviceId = this[TraceBatchTable.deviceId],
            channel = Channel.from(this[TraceBatchTable.channel]),
            pointCount = this[TraceBatchTable.pointCount],
            firstTs = this[TraceBatchTable.firstTs],
            lastTs = this[TraceBatchTable.lastTs],
            clientTs = this[TraceBatchTable.clientTs],
            receivedAt = this[TraceBatchTable.receivedAt],
            sdkVersion = this[TraceBatchTable.sdkVersion],
            platform = this[TraceBatchTable.platform],
            status = TraceBatchStatus.from(this[TraceBatchTable.status]),
            errorCode = this[TraceBatchTable.errorCode],
            processedAt = this[TraceBatchTable.processedAt],
        )

    companion object {
        /** 失败批扫描默认页大小。 */
        const val DEFAULT_LIMIT: Int = 100
    }
}
