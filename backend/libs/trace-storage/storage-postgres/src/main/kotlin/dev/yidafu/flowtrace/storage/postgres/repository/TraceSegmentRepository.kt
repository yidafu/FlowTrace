package dev.yidafu.flowtrace.storage.postgres.repository

import dev.yidafu.flowtrace.model.Bbox
import dev.yidafu.flowtrace.model.MoveMode
import dev.yidafu.flowtrace.model.SegmentKind
import dev.yidafu.flowtrace.model.SegmentStatus
import dev.yidafu.flowtrace.model.TraceSegmentDraft
import dev.yidafu.flowtrace.storage.postgres.table.TraceSegmentTable
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.eq
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.greater
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.greaterEq
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.less
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.core.sum
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import org.springframework.stereotype.Repository
import java.time.Instant
import java.util.UUID

/**
 * 段的身份范围：`(user_id, device_id, session_id)`。
 *
 * 删除/重写窗口必须带 `device_id`：`segment_id` 派生输入里 `session_id` 可为空，
 * 同用户多设备并发写段时不能互相删（08 §6.2「删除范围必须带 deviceId」）。
 */
data class SegmentScope(
    val userId: String,
    val deviceId: String,
    val sessionId: String?,
)

/** 轨迹段查询游标：`(start_ts, segment_id)` keyset 位置。 */
data class TraceSegmentCursor(
    val startTsMs: Long,
    val segmentId: UUID,
)

/**
 * `trace_segment` 的一行（03 §3）。
 *
 * 与 [TraceSegmentDraft] 的区别只在存储侧字段：[status]、[updatedAt] 以及身份列
 * （user/device/session/task）由写入层与表结构持有，草稿只承载「重算两次结果相同」的纯计算字段。
 */
data class TraceSegmentRow(
    val segmentId: UUID,
    val userId: String,
    val deviceId: String,
    val sessionId: String?,
    val taskId: String?,
    val startTsMs: Long,
    val endTsMs: Long,
    val kind: SegmentKind,
    val moveMode: MoveMode,
    val pointCount: Int,
    val distanceM: Double,
    val geometryWkt: String,
    val bbox: Bbox,
    val stayLatQ: Int? = null,
    val stayLngQ: Int? = null,
    val status: SegmentStatus,
    val updatedAt: Instant,
) {
    /** 还原为草稿（重算时「读→改→写回」用）。 */
    fun toDraft(): TraceSegmentDraft =
        TraceSegmentDraft(
            segmentId = segmentId,
            startTsMs = startTsMs,
            endTsMs = endTsMs,
            kind = kind,
            moveMode = moveMode,
            pointCount = pointCount,
            distanceM = distanceM,
            geometryWkt = geometryWkt,
            bbox = bbox,
            stayLatQ = stayLatQ,
            stayLngQ = stayLngQ,
        )
}

/**
 * `trace_segment` 读写仓储（03 §3，按月分区）。
 *
 * 段是**派生数据**（可由 `trace_point` 全量重算，D8/D13），因此本仓储的核心不是「插入」
 * 而是「重写窗口」：[replaceTail] 在同一事务里先删窗口再写入，保证查询方不会看到
 * 「段缺失」的空洞，同时天然支持批重放幂等。
 *
 * geometry 一律 WKT 文本（MOVE = 抽稀 LINESTRING，STAY = 质心 POINT），
 * bbox 四列冗余存是为了让消费方粗筛时不必解析 WKT——空间判断全部在应用层 JTS。
 */
@Repository
open class TraceSegmentRepository {
    /**
     * 重写 `fromTsMs` 起的段窗口（08 §6.2，processor 每次 flush 调用一次）。
     *
     * 同一事务两步：**先删窗口、再批量写入**。
     * - 必须整窗重写而非纯 upsert：STAY 段的 `start_ts` 由停留簇首点决定，
     *   晚到的点可能把前一条 MOVE 段的前半截重判为 STAY，`segment_id` 随之变化，
     *   只 upsert 会留下过期的 MOVE 段；
     * - 删除带 `device_id`（见 [SegmentScope]）；
     * - 写入用 `batchInsert(ignore = true)`：窗口内行已被删空，冲突只可能来自并发重放，
     *   此时丢弃重复行比覆盖更安全（同设备同窗口理论上只有一个消费者）。
     *
     * @param scope 用户 + 设备 + 会话范围
     * @param fromTsMs 窗口首点 ts（含），删除 `start_ts >= fromTsMs` 的段
     * @param drafts 该窗口的全部段（可为空：表示窗口内已无段）
     * @param status 写入状态：未闭合窗口 OPEN，闭合 flush 传 FINALIZED
     */
    fun replaceTail(
        scope: SegmentScope,
        fromTsMs: Long,
        drafts: List<TraceSegmentDraft>,
        status: SegmentStatus = SegmentStatus.OPEN,
    ) {
        val from = Instant.ofEpochMilli(fromTsMs)
        val now = Instant.now()
        transaction {
            // sessionId 为 null 时需要收敛到 `session_id IS NULL`：Exposed 的 eq(null) 会生成 IS NULL
            val sessionCondition: Op<Boolean> = TraceSegmentTable.sessionId eq scope.sessionId
            TraceSegmentTable.deleteWhere {
                (TraceSegmentTable.userId eq scope.userId) and
                    (TraceSegmentTable.deviceId eq scope.deviceId) and
                    (TraceSegmentTable.startTs greaterEq from) and
                    sessionCondition
            }
            if (drafts.isEmpty()) return@transaction
            drafts.chunked(BATCH_SIZE).forEach { chunk ->
                TraceSegmentTable
                    .batchInsert(chunk, ignore = true, shouldReturnGeneratedValues = false) { d ->
                        this[TraceSegmentTable.startTs] = Instant.ofEpochMilli(d.startTsMs)
                        this[TraceSegmentTable.segmentId] = d.segmentId
                        this[TraceSegmentTable.userId] = scope.userId
                        this[TraceSegmentTable.deviceId] = scope.deviceId
                        this[TraceSegmentTable.sessionId] = scope.sessionId
                        this[TraceSegmentTable.kind] = d.kind.value
                        this[TraceSegmentTable.moveMode] = d.moveMode.value
                        this[TraceSegmentTable.endTs] = Instant.ofEpochMilli(d.endTsMs)
                        this[TraceSegmentTable.pointCount] = d.pointCount
                        this[TraceSegmentTable.distanceM] = d.distanceM
                        this[TraceSegmentTable.geometry] = d.geometryWkt
                        this[TraceSegmentTable.stayLatQ] = d.stayLatQ
                        this[TraceSegmentTable.stayLngQ] = d.stayLngQ
                        this[TraceSegmentTable.minLatQ] = d.bbox.minLatQ
                        this[TraceSegmentTable.maxLatQ] = d.bbox.maxLatQ
                        this[TraceSegmentTable.minLngQ] = d.bbox.minLngQ
                        this[TraceSegmentTable.maxLngQ] = d.bbox.maxLngQ
                        this[TraceSegmentTable.status] = status.value
                        this[TraceSegmentTable.updatedAt] = now
                    }
            }
        }
    }

    /**
     * 把 `beforeTsMs` 之前的 OPEN 段收敛为 FINALIZED（08 §8.5 `openTruncated` 恢复路径）。
     *
     * 缓存恢复时窗口被 `openSegMaxPoints` 截断，旧 OPEN 段的起点已不在内存窗口内；
     * 若直接按内存首点 [replaceTail]，旧段既不会被删也不会被更新，会一直停留为 OPEN。
     * 因此先在这里把它封口，再从内存首点重写新窗口。
     *
     * 删除/更新范围同样带 `deviceId`，且**不带 sessionId 的 taskId 等可变列**，
     * 避免任务关联变化后漏掉旧段。
     *
     * @return 被封口的段数
     */
    fun finalizeOpenBefore(
        scope: SegmentScope,
        beforeTsMs: Long,
    ): Int =
        transaction {
            val sessionCondition: Op<Boolean> = TraceSegmentTable.sessionId eq scope.sessionId
            val closedBefore = Instant.ofEpochMilli(beforeTsMs - 1)
            val now = Instant.now()
            TraceSegmentTable.update(
                where = {
                    (TraceSegmentTable.userId eq scope.userId) and
                        (TraceSegmentTable.deviceId eq scope.deviceId) and
                        sessionCondition and
                        (TraceSegmentTable.status eq SegmentStatus.OPEN.value) and
                        (TraceSegmentTable.startTs less Instant.ofEpochMilli(beforeTsMs))
                },
            ) {
                it[status] = SegmentStatus.FINALIZED.value
                it[endTs] = closedBefore
                it[updatedAt] = now
            }
        }

    /**
     * 用户最近的一条 OPEN 段（processor 缓存 miss 时恢复在线窗口，08 §8.1 第 2 步）。
     *
     * 走部分索引 `trace_segment_user_status_idx`（`status = 1`）。
     */
    fun findLatestOpen(userId: String): TraceSegmentRow? =
        transaction {
            TraceSegmentTable
                .selectAll()
                .where {
                    (TraceSegmentTable.userId eq userId) and
                        (TraceSegmentTable.status eq SegmentStatus.OPEN.value)
                }.orderBy(
                    TraceSegmentTable.startTs to SortOrder.DESC,
                    TraceSegmentTable.segmentId to SortOrder.DESC,
                ).limit(1)
                .map { it.toTraceSegmentRow() }
                .singleOrNull()
        }

    /**
     * 用户时间范围内的段（`GET /v1/trace-segments`，06 §5.1）。
     *
     * 时间过滤走 `start_ts`（分区键 + 索引前缀）；`kind`/`sessionId`/`taskId` 为可选过滤，
     * 巡检业务只经 `sessionId`/`taskId` 关联，不侵入上报主链路。
     */
    fun listByUser(
        userId: String,
        fromMsInclusive: Long,
        toMsExclusive: Long,
        kind: SegmentKind? = null,
        sessionId: String? = null,
        taskId: String? = null,
        limit: Int = DEFAULT_LIMIT,
        after: TraceSegmentCursor? = null,
    ): List<TraceSegmentRow> =
        transaction {
            var condition: Op<Boolean> =
                (TraceSegmentTable.userId eq userId) and
                    (TraceSegmentTable.startTs greaterEq Instant.ofEpochMilli(fromMsInclusive)) and
                    (TraceSegmentTable.startTs less Instant.ofEpochMilli(toMsExclusive))
            kind?.let { condition = condition and (TraceSegmentTable.kind eq it.value) }
            sessionId?.let { condition = condition and (TraceSegmentTable.sessionId eq it) }
            taskId?.let { condition = condition and (TraceSegmentTable.taskId eq it) }
            if (after != null) {
                val cursorTs = Instant.ofEpochMilli(after.startTsMs)
                condition = condition and (
                    (TraceSegmentTable.startTs greater cursorTs) or
                        (
                            (TraceSegmentTable.startTs eq cursorTs) and
                                (TraceSegmentTable.segmentId greater after.segmentId)
                        )
                )
            }
            TraceSegmentTable
                .selectAll()
                .where(condition)
                .orderBy(
                    TraceSegmentTable.startTs to SortOrder.ASC,
                    TraceSegmentTable.segmentId to SortOrder.ASC,
                ).limit(limit)
                .map { it.toTraceSegmentRow() }
        }

    /**
     * 按出行模式汇总 MOVE 段里程（`GET /v1/traces/distance`，06 §5.1）。
     *
     * 只聚合 MOVE 段（STAY 的 `distance_m` 恒为 0）；范围里有 OPEN 段时调用方需
     * 用 [hasOpenInRange] 判定降级到点级 haversine。
     */
    fun sumDistanceByMode(
        userId: String,
        fromMsInclusive: Long,
        toMsExclusive: Long,
    ): Map<MoveMode, Double> =
        transaction {
            val totalDistance = TraceSegmentTable.distanceM.sum()
            TraceSegmentTable
                .select(TraceSegmentTable.moveMode, totalDistance)
                .where {
                    (TraceSegmentTable.userId eq userId) and
                        (TraceSegmentTable.startTs greaterEq Instant.ofEpochMilli(fromMsInclusive)) and
                        (TraceSegmentTable.startTs less Instant.ofEpochMilli(toMsExclusive)) and
                        (TraceSegmentTable.kind eq SegmentKind.MOVE.value)
                }.groupBy(TraceSegmentTable.moveMode)
                .associate { row ->
                    MoveMode.from(row[TraceSegmentTable.moveMode]) to (row[totalDistance] ?: 0.0)
                }
        }

    /** 范围内是否存在未收尾的段（里程口径降级判据，06 §5.1）。 */
    fun hasOpenInRange(
        userId: String,
        fromMsInclusive: Long,
        toMsExclusive: Long,
    ): Boolean =
        transaction {
            TraceSegmentTable
                .selectAll()
                .where {
                    (TraceSegmentTable.userId eq userId) and
                        (TraceSegmentTable.startTs greaterEq Instant.ofEpochMilli(fromMsInclusive)) and
                        (TraceSegmentTable.startTs less Instant.ofEpochMilli(toMsExclusive)) and
                        (TraceSegmentTable.status eq SegmentStatus.OPEN.value)
                }.limit(1)
                .any()
        }

    /**
     * 删除用户轨迹段（03 §10 删除矩阵），按时间范围可选。
     *
     * @return 删除行数
     */
    fun deleteByUser(
        userId: String,
        fromMsInclusive: Long? = null,
        toMsExclusive: Long? = null,
    ): Int =
        transaction {
            var condition: Op<Boolean> = TraceSegmentTable.userId eq userId
            fromMsInclusive?.let { condition = condition and (TraceSegmentTable.startTs greaterEq Instant.ofEpochMilli(it)) }
            toMsExclusive?.let { condition = condition and (TraceSegmentTable.startTs less Instant.ofEpochMilli(it)) }
            TraceSegmentTable.deleteWhere { condition }
        }

    private fun ResultRow.toTraceSegmentRow(): TraceSegmentRow =
        TraceSegmentRow(
            segmentId = this[TraceSegmentTable.segmentId],
            userId = this[TraceSegmentTable.userId],
            deviceId = this[TraceSegmentTable.deviceId],
            sessionId = this[TraceSegmentTable.sessionId],
            taskId = this[TraceSegmentTable.taskId],
            startTsMs = this[TraceSegmentTable.startTs].toEpochMilli(),
            endTsMs = this[TraceSegmentTable.endTs].toEpochMilli(),
            kind = SegmentKind.from(this[TraceSegmentTable.kind]),
            moveMode = MoveMode.from(this[TraceSegmentTable.moveMode]),
            pointCount = this[TraceSegmentTable.pointCount],
            distanceM = this[TraceSegmentTable.distanceM],
            geometryWkt = this[TraceSegmentTable.geometry],
            bbox =
                Bbox(
                    minLatQ = this[TraceSegmentTable.minLatQ],
                    maxLatQ = this[TraceSegmentTable.maxLatQ],
                    minLngQ = this[TraceSegmentTable.minLngQ],
                    maxLngQ = this[TraceSegmentTable.maxLngQ],
                ),
            stayLatQ = this[TraceSegmentTable.stayLatQ],
            stayLngQ = this[TraceSegmentTable.stayLngQ],
            status = SegmentStatus.from(this[TraceSegmentTable.status]),
            updatedAt = this[TraceSegmentTable.updatedAt],
        )

    companion object {
        /** 单次写入分块大小（段数远小于点数，取同一数量级即可）。 */
        const val BATCH_SIZE: Int = 500

        /** 查询默认页大小（04 §3 `limit` 默认 500）。 */
        const val DEFAULT_LIMIT: Int = 500
    }
}
