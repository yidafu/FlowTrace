package dev.yidafu.flowtrace.storage.postgres.repository

import dev.yidafu.flowtrace.model.TracePoint
import dev.yidafu.flowtrace.storage.postgres.table.TracePointTable
import dev.yidafu.flowtrace.storage.postgres.table.toDomain
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.eq
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.greater
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.greaterEq
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.less
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.springframework.stereotype.Repository
import java.time.Instant
import java.util.UUID

/** 轨迹点查询游标：`(ts, point_id)` keyset 分页位置（04 §1 cursor 解码后的结构）。 */
data class TracePointCursor(
    val tsMs: Long,
    val pointId: UUID,
)

/**
 * `trace_point` 读写仓储（03 §2，按天分区，写入/查询两条路径）。
 *
 * 约定：
 * - 写路径只有 [batchInsertIgnore] 一个入口，`ON CONFLICT (ts, point_id) DO NOTHING`
 *   实现点级幂等（pointId 内嵌时间戳，重复点 ts 相同 → 等价全局防重）；
 * - 读路径一律 `(user_id, ts)` 前缀 + keyset 游标，**不做任何地理查询**（空间计算在应用层 JTS）；
 * - 分区裁剪由 PG 按 `ts` 自动完成，跨日范围会被拆到多个子分区，应用层无需感知。
 */
@Repository
open class TracePointRepository {
    /**
     * 批量幂等写点（processor 第 5 步，06 §3.2）。
     *
     * 按 1000 行一批提交，`ignore = true` 生成 `ON CONFLICT DO NOTHING`。
     * **返回的是「尝试写入」行数而非「实际插入」行数**：PG 的 JDBC batch 对
     * `ON CONFLICT DO NOTHING` 返回 `SUCCESS_NO_INFO`，逐行计数拿不到（08 §6.1）。
     * 真实重复率靠离线对账（`count(*)` vs 批申报数）核，不要在热路径再加一次 SELECT。
     *
     * @return 尝试写入的行数（含被冲突丢弃的）
     */
    fun batchInsertIgnore(points: List<TracePoint>): Int {
        if (points.isEmpty()) return 0
        return transaction {
            points.chunked(BATCH_SIZE).sumOf { chunk ->
                TracePointTable
                    .batchInsert(chunk, ignore = true, shouldReturnGeneratedValues = false) { p ->
                        this[TracePointTable.ts] = Instant.ofEpochMilli(p.tsMs)
                        this[TracePointTable.pointId] = p.pointId
                        this[TracePointTable.userId] = p.userId
                        this[TracePointTable.deviceId] = p.deviceId
                        this[TracePointTable.batchId] = p.batchId
                        this[TracePointTable.sessionId] = p.sessionId
                        this[TracePointTable.taskId] = p.taskId
                        this[TracePointTable.latQ] = p.latQ
                        this[TracePointTable.lngQ] = p.lngQ
                        this[TracePointTable.accuracyCm] = p.accuracyCm
                        this[TracePointTable.speedMps] = p.speedMps
                        this[TracePointTable.bearingDeg] = p.bearingDeg
                        this[TracePointTable.altitudeCm] = p.altitudeCm
                        this[TracePointTable.locationSource] = p.source?.value?.toShort()
                        this[TracePointTable.flags] = p.flags
                    }.size
            }
        }
    }

    /**
     * 用户时间范围内的点，`(ts, point_id)` 升序 keyset 分页。
     *
     * 查询 API（`GET /v1/traces`、回放）与迟到重算（读到全天点）共用本方法：
     * 重算时传大 [limit] 或不传 [after]，按 [TracePointCursor] 翻页直到取空。
     *
     * @param fromMsInclusive 起始 ts（含）
     * @param toMsExclusive 结束 ts（不含）
     * @param after 上一页最后一个点的游标；null 表示从 [fromMsInclusive] 开始
     */
    fun findByUser(
        userId: String,
        fromMsInclusive: Long,
        toMsExclusive: Long,
        limit: Int = DEFAULT_LIMIT,
        after: TracePointCursor? = null,
    ): List<TracePoint> =
        transaction {
            TracePointTable
                .selectAll()
                .where { userRangeCondition(userId, fromMsInclusive, toMsExclusive, after) }
                .orderBy(
                    TracePointTable.ts to SortOrder.ASC,
                    TracePointTable.pointId to SortOrder.ASC,
                ).limit(limit)
                .map { it.toDomain() }
        }

    /**
     * 设备最近一个点（processor 缓存 miss 时恢复清洗状态，08 §8.1 第 1 步）。
     *
     * 走 `(device_id, ts)` 索引反向扫描；同毫秒多点时按 `point_id` 取最后一个，保证确定性。
     */
    fun findLatestByDevice(deviceId: String): TracePoint? =
        transaction {
            TracePointTable
                .selectAll()
                .where { TracePointTable.deviceId eq deviceId }
                .orderBy(
                    TracePointTable.ts to SortOrder.DESC,
                    TracePointTable.pointId to SortOrder.DESC,
                ).limit(1)
                .map { it.toDomain() }
                .singleOrNull()
        }

    /** 用户时间范围内的点数（对账/里程降级判定用；分区裁剪后是纯索引扫描）。 */
    fun countByUser(
        userId: String,
        fromMsInclusive: Long,
        toMsExclusive: Long,
    ): Long =
        transaction {
            TracePointTable
                .selectAll()
                .where { userRangeCondition(userId, fromMsInclusive, toMsExclusive, null) }
                .count()
        }

    /**
     * 删除用户轨迹点（03 §10 删除矩阵），按时间范围可选。
     *
     * 分区表的 `DELETE` 会逐子分区执行；作业按天分段调用本方法，让每个分区各自成事务、
     * 断点续跑（删除本身幂等，重跑安全）。
     *
     * @return 删除行数
     */
    fun deleteByUser(
        userId: String,
        fromMsInclusive: Long? = null,
        toMsExclusive: Long? = null,
    ): Int =
        transaction {
            var condition: Op<Boolean> = TracePointTable.userId eq userId
            fromMsInclusive?.let { condition = condition and (TracePointTable.ts greaterEq Instant.ofEpochMilli(it)) }
            toMsExclusive?.let { condition = condition and (TracePointTable.ts less Instant.ofEpochMilli(it)) }
            TracePointTable.deleteWhere { condition }
        }

    /** 用户 + 时间范围 + 可选游标的条件组合（读路径唯一事实，避免两处写法漂移）。 */
    private fun userRangeCondition(
        userId: String,
        fromMsInclusive: Long,
        toMsExclusive: Long,
        after: TracePointCursor?,
    ): Op<Boolean> {
        var condition: Op<Boolean> =
            (TracePointTable.userId eq userId) and
                (TracePointTable.ts greaterEq Instant.ofEpochMilli(fromMsInclusive)) and
                (TracePointTable.ts less Instant.ofEpochMilli(toMsExclusive))
        if (after != null) {
            val cursorTs = Instant.ofEpochMilli(after.tsMs)
            condition = condition and (
                (TracePointTable.ts greater cursorTs) or
                    ((TracePointTable.ts eq cursorTs) and (TracePointTable.pointId greater after.pointId))
            )
        }
        return condition
    }

    companion object {
        /** JDBC batch 分块大小：与 03 §2「JDBC batch 1000」一致。 */
        const val BATCH_SIZE: Int = 1000

        /** 查询默认页大小（04 §3 `limit` 默认 2000）。 */
        const val DEFAULT_LIMIT: Int = 2000
    }
}
