package dev.yidafu.flowtrace.shared.persistence.repository

import dev.yidafu.flowtrace.shared.model.Bbox
import dev.yidafu.flowtrace.shared.model.MoveMode
import dev.yidafu.flowtrace.shared.model.SegmentKind
import dev.yidafu.flowtrace.shared.model.SegmentStatus
import dev.yidafu.flowtrace.shared.model.TraceSegmentDraft
import dev.yidafu.flowtrace.shared.persistence.statement.MultiRowInsertIgnoreStatement
import dev.yidafu.flowtrace.shared.persistence.table.TraceSegmentTable
import org.jetbrains.exposed.v1.core.Op
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.eq
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.greater
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.greaterEq
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.inList
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.less
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.core.or
import org.jetbrains.exposed.v1.core.sum
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.statements.BatchInsertBlockingExecutable
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
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
     * - 写入用单条多行 `INSERT ... ON CONFLICT (start_ts, segment_id) DO NOTHING`（见 [insertDraftsInTx]）：
     *   窗口内行已被删空，冲突只可能来自并发重放，此时丢弃重复行比覆盖更安全（同设备同窗口理论上只有一个消费者）。
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
        transaction {
            // sessionId 为 null 时需要收敛到 `session_id IS NULL`：Exposed 的 eq(null) 会生成 IS NULL
            val sessionCondition: Op<Boolean> = TraceSegmentTable.sessionId eq scope.sessionId
            TraceSegmentTable.deleteWhere {
                (TraceSegmentTable.userId eq scope.userId) and
                    (TraceSegmentTable.deviceId eq scope.deviceId) and
                    (TraceSegmentTable.startTs greaterEq from) and
                    sessionCondition
            }
            insertDraftsInTx(scope, drafts, status)
        }
    }

    /**
     * 只写段草稿、**不删旧行**（迟到重算用，06 §3.6 / 08 §9）。
     *
     * 重算的口径是「先按 `(user_id, day)` 整体删掉该日段（[deleteByUser] 带范围），
     * 再按 `(device, session)` 分组重建」，所以这里不能复用 [replaceTail]——
     * 它的 `startTs >= fromTsMs` 无上界，在按天重算时会连带删掉次日及以后的段。
     *
     * 不提供删除是为了让调用方把「删该日全部设备」与「按设备重建」分成两步，
     * 中间不做事务嵌套（删除是整用户范围，重建是按设备分组，粒度不同）。
     *
     * @param status 重算产物一律 FINALIZED（离线无 OPEN 状态，06 §3.6）
     */
    fun insertDrafts(
        scope: SegmentScope,
        drafts: List<TraceSegmentDraft>,
        status: SegmentStatus = SegmentStatus.FINALIZED,
    ) {
        if (drafts.isEmpty()) return
        transaction { insertDraftsInTx(scope, drafts, status) }
    }

    /**
     * 段草稿落库的公共实现；**调用方必须已在事务内**（`replaceTail` 与 `insertDrafts` 共用）。
     *
     * **单条多行** `INSERT ... VALUES (...),(...) ON CONFLICT (start_ts, segment_id) DO NOTHING`，
     * 由 [MultiRowInsertIgnoreStatement] 生成与参数绑定（列序、引号与值类型都走
     * [TraceSegmentTable] 各列的定义，不再手写 SQL 拼接）。
     *
     * 为什么不用 Exposed 的 `batchInsert(ignore = true)`：那是 JDBC batch（N 行 = N 条 INSERT），
     * pgjdbc 的 `reWriteBatchedInserts` **不会改写带 `ON CONFLICT` 的语句**（URL 里已配 true 也没用），
     * 与 `TracePointRepository.batchInsertIgnore` 同款问题——逐行 vs 多行单语句实测差 10 倍
     * （docs/reports/2026-11-21-4c8g-verification.md §8）。
     */
    private fun insertDraftsInTx(
        scope: SegmentScope,
        drafts: List<TraceSegmentDraft>,
        status: SegmentStatus,
    ) {
        if (drafts.isEmpty()) return
        val now = Instant.now()
        drafts.chunked(BATCH_SIZE).forEach { chunk ->
            val stmt =
                MultiRowInsertIgnoreStatement(
                    table = TraceSegmentTable,
                    conflictKeys = listOf(TraceSegmentTable.startTs, TraceSegmentTable.segmentId),
                )
            for (d in chunk) {
                // addBatch() 先于赋值：Exposed 的 addBatch 会把当前 values 快照进 data 再换出新行
                stmt.addBatch()
                stmt[TraceSegmentTable.startTs] = Instant.ofEpochMilli(d.startTsMs)
                stmt[TraceSegmentTable.segmentId] = d.segmentId
                stmt[TraceSegmentTable.userId] = scope.userId
                stmt[TraceSegmentTable.deviceId] = scope.deviceId
                stmt[TraceSegmentTable.sessionId] = scope.sessionId
                stmt[TraceSegmentTable.kind] = d.kind.value
                stmt[TraceSegmentTable.moveMode] = d.moveMode.value
                stmt[TraceSegmentTable.endTs] = Instant.ofEpochMilli(d.endTsMs)
                stmt[TraceSegmentTable.pointCount] = d.pointCount
                stmt[TraceSegmentTable.distanceM] = d.distanceM
                stmt[TraceSegmentTable.geometry] = d.geometryWkt
                stmt[TraceSegmentTable.stayLatQ] = d.stayLatQ
                stmt[TraceSegmentTable.stayLngQ] = d.stayLngQ
                stmt[TraceSegmentTable.minLatQ] = d.bbox.minLatQ
                stmt[TraceSegmentTable.maxLatQ] = d.bbox.maxLatQ
                stmt[TraceSegmentTable.minLngQ] = d.bbox.minLngQ
                stmt[TraceSegmentTable.maxLngQ] = d.bbox.maxLngQ
                stmt[TraceSegmentTable.status] = status.value
                stmt[TraceSegmentTable.updatedAt] = now
            }
            BatchInsertBlockingExecutable(stmt).execute(TransactionManager.current())
        }
    }

    /**
     * 按天整体重写用户的段（迟到重算，06 §3.6）：**单事务内「删该日全部段 → 按设备重建」**。
     *
     * 为什么必须一个事务：分两步（先 `deleteByUser` 再逐组 `insertDrafts`）会让查询方在删与写之间
     * 看到几秒的「段缺失」空洞；重算是后台作业，没必要付出这个可见性代价。
     *
     * 删除范围是**整用户**的 `[from, to)`：重算后段数可能变少（相邻 STAY 合并、噪声点被清洗），
     * `segmentId` 派生自 `startTs`，旧行不会被新草稿覆盖，不删就会残留幽灵段。
     * 每个 `(device, session)` 分组的草稿由调用方先按天过滤，本方法不再裁剪。
     *
     * @param groups 分组草稿；key 必须带 deviceId 与 sessionId（同用户多设备/多会话不能互相覆盖）
     * @return 实际写入的草稿行数
     */
    fun replaceDayRange(
        userId: String,
        fromMsInclusive: Long,
        toMsExclusive: Long,
        groups: Map<SegmentScope, List<TraceSegmentDraft>>,
        status: SegmentStatus = SegmentStatus.FINALIZED,
    ): Int =
        transaction {
            val from = Instant.ofEpochMilli(fromMsInclusive)
            val to = Instant.ofEpochMilli(toMsExclusive)
            TraceSegmentTable.deleteWhere {
                (TraceSegmentTable.userId eq userId) and
                    (TraceSegmentTable.startTs greaterEq from) and
                    (TraceSegmentTable.startTs less to)
            }
            groups.entries.sumOf { (scope, drafts) ->
                insertDraftsInTx(scope, drafts, status)
                drafts.size
            }
        }

    /**
     * 兜底收尾：把 `end_ts < beforeTsMs` 的 OPEN 段批量封口（06 §3.4「另有定时扫描兜底」）。
     *
     * 在线链路靠「窗口末点落后水位超过乱序窗口」自然收尾，但设备**彻底沉默**时不会再有下一批来触发，
     * 那段轨迹就永远停在 OPEN——查询侧看到的是「未完成段」，里程接口还会降级到点级计算。
     *
     * 分两步而不是一条 `UPDATE ... LIMIT`：Exposed 的 `update` 不支持 LIMIT，
     * 先按主键捞一批 id 再按 id 更新，能把单次锁行数框住（这是兜底作业，不能长事务扫表）。
     *
     * @param beforeTsMs 早于该时间的段视为可收尾（调用方传 `now − 乱序窗口`）
     * @param limit 单次最多封口多少段
     * @return 实际封口段数；小于 [limit] 说明已扫空
     */
    fun finalizeStaleOpen(
        beforeTsMs: Long,
        limit: Int = DEFAULT_FINALIZE_LIMIT,
    ): Int {
        if (limit <= 0) return 0
        val threshold = Instant.ofEpochMilli(beforeTsMs)
        return transaction {
            val ids =
                TraceSegmentTable
                    .selectAll()
                    .where {
                        (TraceSegmentTable.status eq SegmentStatus.OPEN.value) and
                            (TraceSegmentTable.endTs less threshold)
                    }.orderBy(TraceSegmentTable.endTs to SortOrder.ASC)
                    .limit(limit)
                    .map { it[TraceSegmentTable.segmentId] }
            if (ids.isEmpty()) return@transaction 0
            val now = Instant.now()
            TraceSegmentTable.update({
                (TraceSegmentTable.segmentId inList ids) and
                    (TraceSegmentTable.status eq SegmentStatus.OPEN.value)
            }) {
                it[status] = SegmentStatus.FINALIZED.value
                it[updatedAt] = now
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

        /** 兜底收尾单批上限：兜底作业不追吞吐，只求不长时间锁行。 */
        const val DEFAULT_FINALIZE_LIMIT: Int = 500
    }
}
