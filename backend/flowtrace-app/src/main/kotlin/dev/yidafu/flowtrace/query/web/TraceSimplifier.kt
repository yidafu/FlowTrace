package dev.yidafu.flowtrace.query.web

import dev.yidafu.flowtrace.shared.model.GeoMath
import dev.yidafu.flowtrace.shared.model.TracePoint
import org.springframework.stereotype.Component

/**
 * 查询侧简化管道（06 §5.2）：缩放级别 → DP 容差 → 点数兜底等距抽取。
 *
 * 抽稀在米制平面做（[GeoMath.dpSimplifyMeters]），本类只决定容差和兜底取样位置，
 * 不自行实现几何算法。
 */
@Component
class TraceSimplifier {
    /** 06 §5.2 的 zoom→ε LUT；≤10→100m，11–12→50m，13–14→20m，15–16→10m，≥17→3m。 */
    fun lut(zoom: Int): Double =
        when {
            zoom <= 10 -> 100.0
            zoom <= 12 -> 50.0
            zoom <= 14 -> 20.0
            zoom <= 16 -> 10.0
            else -> 3.0
        }

    fun simplify(
        points: List<TracePoint>,
        zoom: Int,
        maxPoint: Int,
    ): List<TracePoint> {
        require(maxPoint > 0) { "maxPoints 必须大于 0" }
        val kept = GeoMath.dpSimplifyMeters(points, epsilonM = lut(zoom))
        // 兜底抽样必须作用在 DP 结果上，否则会把已被剔除的点又放回来。
        return if (kept.size > maxPoint) evenlySample(kept, maxPoint) else kept
    }

    /** 从 [points] 等距取 [maxPoint] 个点，首尾必留。 */
    fun evenlySample(
        points: List<TracePoint>,
        maxPoint: Int,
    ): List<TracePoint> {
        require(maxPoint > 0) { "maxPoints 必须大于 0" }
        if (points.size <= maxPoint) return points
        if (maxPoint == 1) return listOf(points.first(), points.last())
        val lastIndex = points.lastIndex
        return List(maxPoint) { i -> points[i * lastIndex / (maxPoint - 1)] }
    }
}
