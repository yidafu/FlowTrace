package dev.yidafu.flowtrace.process.segment

import dev.yidafu.flowtrace.shared.model.Bbox
import dev.yidafu.flowtrace.shared.model.GeoMath
import dev.yidafu.flowtrace.shared.model.Ids
import dev.yidafu.flowtrace.shared.model.MoveMode
import dev.yidafu.flowtrace.shared.model.SegmentKind
import dev.yidafu.flowtrace.shared.model.TracePoint
import dev.yidafu.flowtrace.shared.model.TraceSegmentDraft
import org.springframework.stereotype.Component

/**
 * 段草稿构建器（06 §3.4 / 08 §8.3）：纯函数，不碰数据库、不持有跨批状态。
 *
 * 输入是一段连续的已清洗点列（通常是 `OpenWindow.points` 或重算时的一整天点），
 * 输出是按 startTs 递增、无缝覆盖输入下标的 STAY/MOVE 交替区间。
 * [TraceSegmentDraft] 不含 status/updatedAt，保证同一批点重算两次结果完全相同。
 *
 * 阈值全部由调用方从 `flowtrace.processor.*` 传入，类内不放默认值，
 * 避免模型/算法层与配置形成第二份事实源。
 */
@Component
class SegmentBuilder {
    private val classifier = MoveModeClassifier()
    private val detector = StayDetector()

    /**
     * @param sessionId 采集会话；协议缺省为空串，这里统一收敛为 null 后再派生 segmentId
     * @param epsilonM 落库几何抽稀 ε（米）
     * @param stayRadiusM 停留判定半径（米）
     * @param stayMinMs 最短停留时长（毫秒）
     * @param stayMergeGapMs 相邻停留合并间隔（毫秒）
     * @param reusable 上一轮算过的草稿（按 `startTsMs` 索引）：区间首末点与点数都没变时直接复用，
     *   跳过 DP 抽稀、里程累加与 WKT 拼串。窗口是追加式的，新批只会改写尾部几个区间，
     *   所以前面的段每批都能命中——这是在线 flush 从「整窗重算」降成「只算尾巴」的关键。
     *   复用是**逐字节等价**的：草稿只由 `(points[区间], ε, userId, sessionId)` 决定，
     *   区间相同则结果必然相同（不引入任何与时间/顺序相关的缓存语义）。
     */
    fun build(
        userId: String,
        sessionId: String?,
        points: List<TracePoint>,
        epsilonM: Double,
        stayRadiusM: Double,
        stayMinMs: Long,
        stayMergeGapMs: Long,
        reusable: Map<Long, TraceSegmentDraft> = emptyMap(),
    ): List<TraceSegmentDraft> {
        if (points.isEmpty()) return emptyList()
        val normalizedSessionId = sessionId?.takeIf { it.isNotEmpty() }
        val stays = detector.detect(points, stayRadiusM, stayMinMs, stayMergeGapMs)
        val intervals = buildIntervals(points.size, stays)
        return intervals.map { interval ->
            toDraft(userId, normalizedSessionId, points, epsilonM, interval, reusable)
        }
    }

    /** 把停留区间切成 MOVE/STAY 交替段；STAY 之间的小段 MOVE 也保留，保证点数守恒。 */
    private fun buildIntervals(
        size: Int,
        stays: List<StayDetector.Stay>,
    ): List<Interval> {
        val out = ArrayList<Interval>()
        var cursor = 0
        for (stay in stays) {
            if (cursor < stay.startIdx) {
                out += Interval(cursor, stay.startIdx - 1, SegmentKind.MOVE)
            }
            out += Interval(stay.startIdx, stay.endIdx, SegmentKind.STAY)
            cursor = stay.endIdx + 1
        }
        if (cursor < size) {
            out += Interval(cursor, size - 1, SegmentKind.MOVE)
        }
        return out
    }

    private fun toDraft(
        userId: String,
        sessionId: String?,
        points: List<TracePoint>,
        epsilonM: Double,
        interval: Interval,
        reusable: Map<Long, TraceSegmentDraft>,
    ): TraceSegmentDraft {
        val part = points.subList(interval.startIdx, interval.endIdx + 1)
        val startTsMs = part.first().tsMs
        val endTsMs = part.last().tsMs

        // 同区间判定：起点 ts 唯一（窗口内点严格递增），再加末点 ts、点数与段类型就能锁死区间边界；
        // 类型也一起比，是为了不给「同区间从 MOVE 重判为 STAY」留任何复用机会
        reusable[startTsMs]?.let { prev ->
            if (prev.kind == interval.kind && prev.endTsMs == endTsMs && prev.pointCount == part.size) return prev
        }

        val segmentId = Ids.segmentId(userId, sessionId, startTsMs)
        val bbox = Bbox.of(part)

        return if (interval.kind == SegmentKind.STAY) {
            val centroid = GeoMath.centroidQ(part)
            TraceSegmentDraft(
                segmentId = segmentId,
                startTsMs = startTsMs,
                endTsMs = endTsMs,
                kind = SegmentKind.STAY,
                moveMode = MoveMode.STILL,
                pointCount = part.size,
                distanceM = 0.0,
                geometryWkt = GeoMath.pointWkt(centroid[0], centroid[1]),
                bbox = bbox,
                stayLatQ = centroid[0],
                stayLngQ = centroid[1],
            )
        } else {
            // 里程用原始点累加，避免抽稀损失；几何用抽稀点，单点几何自动退化为 POINT。
            val simplified = GeoMath.dpSimplifyMeters(part, epsilonM)
            TraceSegmentDraft(
                segmentId = segmentId,
                startTsMs = startTsMs,
                endTsMs = endTsMs,
                kind = SegmentKind.MOVE,
                moveMode = classifier.majority(part),
                pointCount = part.size,
                distanceM = GeoMath.pathLengthM(part),
                geometryWkt = GeoMath.lineStringWkt(simplified),
                bbox = bbox,
            )
        }
    }

    private data class Interval(
        val startIdx: Int,
        val endIdx: Int,
        val kind: SegmentKind,
    )
}
