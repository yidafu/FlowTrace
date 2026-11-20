package dev.yidafu.flowtrace.shared.model

import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier
import java.math.BigDecimal
import kotlin.math.asin
import kotlin.math.atan2
import kotlin.math.cos
import kotlin.math.min
import kotlin.math.sin
import kotlin.math.sqrt

/**
 * 唯一几何工具（06 §1.1 / 决策 D7）：全服务共用，纯函数、无状态、不碰数据库。
 *
 * 设计口径：
 * - 入参出参都用**量化整数** `q = round(度 × 10⁷)`，避免链路上出现 double；
 * - 距离/抽稀在**米制**下算：先用等距圆柱投影把点摊到局部平面（`x = dLng·cos(originLat)·R`），
 *   在平面上做 DP，再反投影回 q。城市尺度（≤50km）误差 <0.1%，远小于 5m 的抽稀 ε；
 * - DP 用 JTS `DouglasPeuckerSimplifier`（AGENTS.md 指定），但**返回的是原始点对象**：
 *   JTS 输出的是输入顶点的子集（内部 `new Coordinate(pts[i])` 值拷贝，值精确相等），
 *   这里用单调双指针把简化结果映射回原索引，保证调用方拿到的点与 `trace_point` 行一一对应。
 */
object GeoMath {
    /** 地球平均半径（米），与 06 §1.1 一致；换值等于换里程口径，需同步全链路。 */
    const val EARTH_RADIUS_M: Double = 6_371_008.8

    /** 坐标量化倍率：度 × 10⁷ → int32。 */
    const val QUANTIZE_SCALE: Double = 1e7

    private val DEG_TO_RAD = Math.PI / 180.0
    private val RAD_TO_DEG = 180.0 / Math.PI
    private val GEOMETRY_FACTORY = GeometryFactory()

    // ------------------------------------------------------------------ 量化

    /** 十进制度 → 量化整数，`Math.round` 语义（.5 向上），与端侧一致。 */
    fun quantize(deg: Double): Int = Math.round(deg * QUANTIZE_SCALE).toInt()

    /** 量化整数 → 十进制度。只用于计算与输出，不用于存储。 */
    fun dequantize(q: Int): Double = q / QUANTIZE_SCALE

    // ------------------------------------------------------------------ 距离与方位

    /**
     * 大圆距离（haversine），单位米。
     *
     * 用 `asin(min(1, sqrt(h)))` 而非 `atan2`：对跖点附近数值更稳，且省一次开方分支。
     */
    fun haversineM(
        aLatQ: Int,
        aLngQ: Int,
        bLatQ: Int,
        bLngQ: Int,
    ): Double {
        val lat1 = dequantize(aLatQ) * DEG_TO_RAD
        val lat2 = dequantize(bLatQ) * DEG_TO_RAD
        val dLat = lat2 - lat1
        val dLng = (dequantize(bLngQ) - dequantize(aLngQ)) * DEG_TO_RAD
        val sinHalfLat = sin(dLat / 2)
        val sinHalfLng = sin(dLng / 2)
        val h = sinHalfLat * sinHalfLat + cos(lat1) * cos(lat2) * sinHalfLng * sinHalfLng
        return 2 * EARTH_RADIUS_M * asin(min(1.0, sqrt(h)))
    }

    /** 两点距离（米），[TracePoint] 便捷重载；不分配中间对象。 */
    fun distanceM(
        a: TracePoint,
        b: TracePoint,
    ): Double = haversineM(a.latQ, a.lngQ, b.latQ, b.lngQ)

    /**
     * 初始方位角（度，0..360，正北为 0、顺时针）。
     *
     * 只用于诊断/展示，**不参与分段与里程**——段边界判定用距离与时间，不用航向。
     */
    fun initialBearingDeg(
        aLatQ: Int,
        aLngQ: Int,
        bLatQ: Int,
        bLngQ: Int,
    ): Double {
        val lat1 = dequantize(aLatQ) * DEG_TO_RAD
        val lat2 = dequantize(bLatQ) * DEG_TO_RAD
        val dLng = (dequantize(bLngQ) - dequantize(aLngQ)) * DEG_TO_RAD
        val y = sin(dLng) * cos(lat2)
        val x = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dLng)
        val deg = atan2(y, x) * RAD_TO_DEG
        return (deg + 360.0) % 360.0
    }

    /** 折线里程（米）：相邻点 haversine 累加。0/1 个点返回 0。 */
    fun pathLengthM(points: List<TracePoint>): Double {
        var sum = 0.0
        for (i in 1 until points.size) {
            sum += haversineM(points[i - 1].latQ, points[i - 1].lngQ, points[i].latQ, points[i].lngQ)
        }
        return sum
    }

    // ------------------------------------------------------------------ 局部投影

    /**
     * 等距圆柱局部投影（06 §1.1）：以 [originLatQ]/[originLngQ] 为原点，把点摊到米制平面。
     *
     * 返回**交错数组** `[x0, y0, x1, y1, ...]`（x 东向、y 北向），而不是 `List<Point>`：
     * 单段最多几千点，避免为每个点分配对象；调用方按 `i * 2` / `i * 2 + 1` 取值。
     */
    fun toLocalMeters(
        originLatQ: Int,
        originLngQ: Int,
        points: List<TracePoint>,
    ): DoubleArray {
        val originLat = dequantize(originLatQ)
        val originLng = dequantize(originLngQ)
        val cosLat = cos(originLat * DEG_TO_RAD)
        val out = DoubleArray(points.size * 2)
        for (i in points.indices) {
            val p = points[i]
            out[i * 2] = (dequantize(p.lngQ) - originLng) * DEG_TO_RAD * cosLat * EARTH_RADIUS_M
            out[i * 2 + 1] = (dequantize(p.latQ) - originLat) * DEG_TO_RAD * EARTH_RADIUS_M
        }
        return out
    }

    /**
     * 局部米制坐标反投影回量化坐标，返回交错数组 `[latQ0, lngQ0, latQ1, lngQ1, ...]`。
     *
     * 与 [toLocalMeters] 成对使用（DP 反向映射、质心/几何点回填）。
     * 高纬度 `cos(lat)` 趋 0 时投影本身失真，这里钳制下限避免除零产生 Infinity。
     */
    fun localToQuantized(
        originLatQ: Int,
        originLngQ: Int,
        local: DoubleArray,
    ): IntArray {
        val originLat = dequantize(originLatQ)
        val originLng = dequantize(originLngQ)
        val cosLat = cos(originLat * DEG_TO_RAD).coerceAtLeast(1e-9)
        val count = local.size / 2
        val out = IntArray(count * 2)
        for (i in 0 until count) {
            val lngDeg = originLng + local[i * 2] / (EARTH_RADIUS_M * cosLat) * RAD_TO_DEG
            val latDeg = originLat + local[i * 2 + 1] / EARTH_RADIUS_M * RAD_TO_DEG
            out[i * 2] = quantize(latDeg)
            out[i * 2 + 1] = quantize(lngDeg)
        }
        return out
    }

    // ------------------------------------------------------------------ 抽稀

    /**
     * Douglas-Peucker 抽稀（06 §1.1，存储几何固定 ε=5m）。
     *
     * 返回输入点的**子序列**（保持原顺序、首尾必留），可直接用于拼 LINESTRING，
     * 也可用于点位抽稀（查询侧 LUT 用不同 ε 调同一函数）。
     *
     * 边界：≤2 个点、ε≤0、非有限 ε → 原样返回；简化结果退化（<2 顶点）→ 保留首尾。
     */
    fun dpSimplifyMeters(
        points: List<TracePoint>,
        epsilonM: Double,
    ): List<TracePoint> {
        if (points.size <= 2 || epsilonM <= 0.0 || !epsilonM.isFinite()) return points.toList()
        val local = toLocalMeters(points.first().latQ, points.first().lngQ, points)
        val coords = Array(points.size) { Coordinate(local[it * 2], local[it * 2 + 1]) }
        val simplified = DouglasPeuckerSimplifier.simplify(GEOMETRY_FACTORY.createLineString(coords), epsilonM)
        val kept = simplified.coordinates
        if (kept.size < 2) return listOf(points.first(), points.last())

        // JTS 只删顶点、不动坐标，且输出是输入子集（值精确相等）；
        // 用单调游标映射回原索引，顺带保证结果下标严格递增（重复坐标不会回退）。
        val result = ArrayList<TracePoint>(kept.size)
        var cursor = 0
        for (c in kept) {
            while (cursor < points.size && !(local[cursor * 2] == c.x && local[cursor * 2 + 1] == c.y)) {
                cursor++
            }
            check(cursor < points.size) { "DP 简化结果不在输入顶点集内：(${c.x}, ${c.y})" }
            result += points[cursor]
            cursor++
        }
        return result
    }

    // ------------------------------------------------------------------ 质心与 WKT

    /**
     * 点簇质心（量化坐标），返回 `[latQ, lngQ]`。
     *
     * 在 q 上做算术平均（q 与度是线性关系，先平均再反算与先反算再平均结果等价，
     * 但前者无浮点累积误差）；STAY 段用它，不用 JTS 加权质心——语义不同。
     */
    fun centroidQ(points: List<TracePoint>): IntArray {
        require(points.isNotEmpty()) { "质心需要至少一个点" }
        var sumLat = 0L
        var sumLng = 0L
        for (p in points) {
            sumLat += p.latQ
            sumLng += p.lngQ
        }
        val n = points.size.toLong()
        return intArrayOf(
            Math.round(sumLat.toDouble() / n).toInt(),
            Math.round(sumLng.toDouble() / n).toInt(),
        )
    }

    /**
     * 拼 LINESTRING WKT（`LINESTRING(lng lat, lng lat, ...)`，OGC 顺序是**经度在前**）。
     *
     * 数值用 `BigDecimal.valueOf(deg).toPlainString()`：定长 7 位小数会产生大量尾零
     * （10 万段 × 数十顶点），而 double 的最短表示天然压掉尾零且能精确还原 q。
     * 不接受空列表（段至少 1 点）；单点退化为 POINT，避免产出非法 LINESTRING。
     */
    fun lineStringWkt(points: List<TracePoint>): String {
        require(points.isNotEmpty()) { "LINESTRING 需要至少一个点" }
        if (points.size == 1) return pointWkt(points[0].latQ, points[0].lngQ)
        val sb = StringBuilder(points.size * 16)
        sb.append("LINESTRING(")
        points.forEachIndexed { i, p ->
            if (i > 0) sb.append(", ")
            appendLngLat(sb, p.lngQ, p.latQ)
        }
        sb.append(')')
        return sb.toString()
    }

    /** 拼 POINT WKT（`POINT(lng lat)`），STAY 段质心用。 */
    fun pointWkt(
        latQ: Int,
        lngQ: Int,
    ): String {
        val sb = StringBuilder(24)
        sb.append("POINT(")
        appendLngLat(sb, lngQ, latQ)
        sb.append(')')
        return sb.toString()
    }

    private fun appendLngLat(
        sb: StringBuilder,
        lngQ: Int,
        latQ: Int,
    ) {
        sb.append(BigDecimal.valueOf(dequantize(lngQ)).toPlainString())
        sb.append(' ')
        sb.append(BigDecimal.valueOf(dequantize(latQ)).toPlainString())
    }
}
