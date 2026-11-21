package dev.yidafu.flowtrace.query.dto

/**
 * GeoJSON LineString（04 §3 / openapi `TraceQueryGeoJsonResponse.geometry`）。
 *
 * 坐标顺序是 `[lng, lat]`、十进制度 double，由量化整数 `/1e7` 还原；
 * 时间不内嵌坐标，通过平行的 [timestampsMs] 数组一一对应。
 */
data class LineString(
    val type: String = "LineString",
    val coordinates: List<List<Double>>,
)

/**
 * `format=geojson` 的轨迹点响应。
 *
 * [originalPoints] 是本页从仓储读到、简化前的点数；[returnedPoints] 是本次实际返回的点数。
 * 两者都以「页」为口径，不额外做全范围 count（06 §5.4 的 P95 预算不含该查询）。
 */
data class TracePointsGeojsonResponse(
    val userId: String,
    val start: String,
    val end: String,
    val simplify: Boolean,
    val zoom: Int,
    val format: String = TraceQueryFormat.GEOJSON.value,
    val geometry: LineString,
    val timestampsMs: List<Long>,
    val returnedPoints: Int,
    val originalPoints: Int,
    val nextCursor: String?,
    val hasMore: Boolean,
)
