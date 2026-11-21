package dev.yidafu.flowtrace.query.service

import dev.yidafu.flowtrace.query.web.CursorCodec
import dev.yidafu.flowtrace.shared.model.TracePoint
import dev.yidafu.flowtrace.shared.persistence.repository.TracePointCursor
import dev.yidafu.flowtrace.shared.persistence.repository.TracePointRepository
import org.springframework.stereotype.Service
import java.time.Instant

/** 一页原始点及分页元信息；[originalPoints] 为本页简化前的点数。 */
data class TracePointPage(
    val points: List<TracePoint>,
    val originalPoints: Int,
    val hasMore: Boolean,
    val nextCursor: String?,
)

/**
 * 轨迹点查询（06 §5.1）：委托仓储的 `(user_id, ts, point_id)` keyset 读路径。
 *
 * 之前这里直接查表，既漏选了 [TracePoint] 需要的列，又让 `pointId` 游标形同虚设；
 * 统一游标类型后直接用仓储这一处事实源，避免两处分页语义漂移。
 * 简化（DP LUT / 等距抽取）留给 `TraceSimplifier`，这里只负责多取一条判分页。
 */
@Service
class TraceQueryService(
    private val repository: TracePointRepository,
    private val codec: CursorCodec,
) {
    fun findByUser(
        userId: String,
        start: Instant,
        end: Instant,
        limit: Int,
        cursor: TracePointCursor?,
    ): TracePointPage {
        // 多取一条：看到第 limit+1 个点才能确定 hasMore，省掉一次 count。
        val rows =
            repository.findByUser(
                userId = userId,
                fromMsInclusive = start.toEpochMilli(),
                toMsExclusive = end.toEpochMilli(),
                limit = limit + 1,
                after = cursor,
            )
        val hasMore = rows.size > limit
        val page = if (hasMore) rows.take(limit) else rows
        val nextCursor =
            if (hasMore) {
                page.lastOrNull()?.let { codec.encode(TracePointCursor(it.tsMs, it.pointId)) }
            } else {
                null
            }
        return TracePointPage(
            points = page,
            originalPoints = page.size,
            hasMore = hasMore,
            nextCursor = nextCursor,
        )
    }

    /**
     * 窗口内全部原始点，按 `(ts, point_id)` 升序流式读取（回放、里程降级共用）。
     *
     * 每次只取一页，游标取本页最后一点，[TracePointRepository] 的 keyset 条件保证翻页不漏不重；
     * 返回 [Sequence] 让调用方可 `take` 截断回放取点上限，也能让里程降级逐点累加而不整窗驻留内存。
     * 注意 Sequence 是冷流：每次消费都会重新查库，且消费必须发生在同一请求线程内。
     */
    fun readWindow(
        userId: String,
        start: Instant,
        end: Instant,
        pageSize: Int = WINDOW_PAGE_SIZE,
    ): Sequence<TracePoint> {
        require(pageSize > 0) { "pageSize 必须大于 0" }
        val fromMs = start.toEpochMilli()
        val toMs = end.toEpochMilli()
        return sequence {
            var cursor: TracePointCursor? = null
            while (true) {
                val rows =
                    repository.findByUser(
                        userId = userId,
                        fromMsInclusive = fromMs,
                        toMsExclusive = toMs,
                        limit = pageSize,
                        after = cursor,
                    )
                if (rows.isEmpty()) break
                yieldAll(rows)
                if (rows.size < pageSize) break
                val last = rows.last()
                cursor = TracePointCursor(last.tsMs, last.pointId)
            }
        }
    }

    private companion object {
        const val WINDOW_PAGE_SIZE = 5_000
    }
}
