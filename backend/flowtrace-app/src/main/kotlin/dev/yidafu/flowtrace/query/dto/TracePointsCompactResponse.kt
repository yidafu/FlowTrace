package dev.yidafu.flowtrace.query.dto

/**
 * `format=compact` 的轨迹点响应（openapi `TraceQueryCompactResponse`）。
 *
 * [points] 是 `[tsMs, latE7, lngE7]` 整数三元组，客户端按 `/1e7` 还原坐标；
 * [originalPoints] / [returnedPoints] 的口径与 geojson 响应一致，均为本页。
 */
data class TracePointsCompactResponse(
    val userId: String,
    val start: String,
    val end: String,
    val simplify: Boolean,
    val zoom: Int,
    val format: String = TraceQueryFormat.COMPACT.value,
    val points: List<List<Long>>,
    val returnedPoints: Int,
    val originalPoints: Int,
    val nextCursor: String?,
    val hasMore: Boolean,
)
