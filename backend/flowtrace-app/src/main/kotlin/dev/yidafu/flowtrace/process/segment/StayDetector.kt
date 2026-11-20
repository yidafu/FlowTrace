package dev.yidafu.flowtrace.process.segment

import dev.yidafu.flowtrace.shared.model.GeoMath
import dev.yidafu.flowtrace.shared.model.TracePoint
import org.springframework.stereotype.Component

/**
 * 停留点检测（06 §3.5 双指针，O(n)）。
 *
 * 半径比较基准是**簇锚点** `points[i]`，不是上一点也不是质心——后者会让缓慢漂移
 * 滚成一条线。相邻停留点间隔 ≤ `stayMergeGapMs` 时合并，
 * 合并后以新簇锚点重新检查半径，超限则不合并（避免把两次短停粘成一次长停）。
 */
@Component
class StayDetector {
    /** 停留区间，下标闭区间；合并后的区间包含两次停留之间的点。 */
    data class Stay(
        val startIdx: Int,
        val endIdx: Int,
    ) {
        init {
            require(endIdx >= startIdx) { "STAY 区间下标非法: start=$startIdx, end=$endIdx" }
        }

        val pointCount: Int
            get() = endIdx - startIdx + 1
    }

    /**
     * 显式阈值检测，阈值由调用方（OnlineSegmenter 从 ProcessorProperties）传入，
     * 保证默认值只有一处事实来源。
     *
     * @param radiusM 距锚点半径（米），闭区间比较
     * @param minMs 最短停留时长（毫秒）
     * @param mergeGapMs 相邻停留合并间隔（毫秒）
     */
    fun detect(
        points: List<TracePoint>,
        radiusM: Double,
        minMs: Long,
        mergeGapMs: Long,
    ): List<Stay> {
        if (points.size < 2) return emptyList()

        val raw = ArrayList<Stay>()
        var i = 0
        while (i < points.size) {
            val anchor = points[i]
            var j = i
            while (
                j + 1 < points.size &&
                GeoMath.haversineM(
                    anchor.latQ,
                    anchor.lngQ,
                    points[j + 1].latQ,
                    points[j + 1].lngQ,
                ) <= radiusM
            ) {
                j++
            }
            if (points[j].tsMs - points[i].tsMs >= minMs) {
                raw += Stay(i, j)
                i = j + 1
            } else {
                i++
            }
        }

        val merged = ArrayList<Stay>(raw.size)
        for (candidate in raw) {
            val last = merged.lastOrNull()
            if (last != null && points[candidate.startIdx].tsMs - points[last.endIdx].tsMs <= mergeGapMs) {
                val trial = Stay(last.startIdx, candidate.endIdx)
                if (withinRadius(points, trial, radiusM)) {
                    merged[merged.lastIndex] = trial
                    continue
                }
            }
            merged += candidate
        }
        return merged
    }

    /** 合并候选是否仍全部落在新锚点的半径内。 */
    private fun withinRadius(
        points: List<TracePoint>,
        stay: Stay,
        radiusM: Double,
    ): Boolean {
        val anchor = points[stay.startIdx]
        for (i in stay.startIdx..stay.endIdx) {
            val p = points[i]
            if (GeoMath.haversineM(anchor.latQ, anchor.lngQ, p.latQ, p.lngQ) > radiusM) return false
        }
        return true
    }
}
