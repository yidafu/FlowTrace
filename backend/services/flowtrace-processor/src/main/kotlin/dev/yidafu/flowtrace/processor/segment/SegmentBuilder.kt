package dev.yidafu.flowtrace.processor.segment

import dev.yidafu.flowtrace.model.Bbox
import dev.yidafu.flowtrace.model.GeoMath
import dev.yidafu.flowtrace.model.Ids
import dev.yidafu.flowtrace.model.MoveMode
import dev.yidafu.flowtrace.model.SegmentKind
import dev.yidafu.flowtrace.model.TracePoint
import dev.yidafu.flowtrace.model.TraceSegmentDraft
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
     */
    fun build(
        userId: String,
        sessionId: String?,
        points: List<TracePoint>,
        epsilonM: Double,
        stayRadiusM: Double,
        stayMinMs: Long,
        stayMergeGapMs: Long,
    ): List<TraceSegmentDraft> {
        if (points.isEmpty()) return emptyList()
        val normalizedSessionId = sessionId?.takeIf { it.isNotEmpty() }
        val stays = detector.detect(points, stayRadiusM, stayMinMs, stayMergeGapMs)
        val intervals = buildIntervals(points.size, stays)
        return intervals.map { interval ->
            toDraft(userId, normalizedSessionId, points, epsilonM, interval)
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
    ): TraceSegmentDraft {
        val part = points.subList(interval.startIdx, interval.endIdx + 1)
        val startTsMs = part.first().tsMs
        val segmentId = Ids.segmentId(userId, sessionId, startTsMs)
        val bbox = Bbox.of(part)

        return if (interval.kind == SegmentKind.STAY) {
            val centroid = GeoMath.centroidQ(part)
            TraceSegmentDraft(
                segmentId = segmentId,
                startTsMs = startTsMs,
                endTsMs = part.last().tsMs,
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
                endTsMs = part.last().tsMs,
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
