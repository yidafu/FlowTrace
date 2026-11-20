package dev.yidafu.flowtrace.shared.model

/**
 * 量化坐标包围盒（03 §3 `trace_segment` 的 `min/max_lat_q`、`min/max_lng_q` 四列）。
 *
 * 冗余存 bbox 的唯一目的是**消费方粗筛时不必解析 WKT**（巡检系统按范围取段），
 * 所以它必须与 geometry 覆盖同一批点——由 `SegmentBuilder` 在同一处计算，禁止事后修正。
 */
data class Bbox(
    val minLatQ: Int,
    val maxLatQ: Int,
    val minLngQ: Int,
    val maxLngQ: Int,
) {
    /** 点是否落在盒内（含边界）；粗筛用法，不能替代精确几何判定。 */
    fun contains(
        latQ: Int,
        lngQ: Int,
    ): Boolean = latQ in minLatQ..maxLatQ && lngQ in minLngQ..maxLngQ

    companion object {
        /** 空集合的 bbox 无意义，调用方需自行保证 [points] 非空（段至少 1 个点）。 */
        fun of(points: List<TracePoint>): Bbox {
            require(points.isNotEmpty()) { "bbox 需要至少一个点" }
            var minLat = Int.MAX_VALUE
            var maxLat = Int.MIN_VALUE
            var minLng = Int.MAX_VALUE
            var maxLng = Int.MIN_VALUE
            for (p in points) {
                if (p.latQ < minLat) minLat = p.latQ
                if (p.latQ > maxLat) maxLat = p.latQ
                if (p.lngQ < minLng) minLng = p.lngQ
                if (p.lngQ > maxLng) maxLng = p.lngQ
            }
            return Bbox(minLat, maxLat, minLng, maxLng)
        }
    }
}
