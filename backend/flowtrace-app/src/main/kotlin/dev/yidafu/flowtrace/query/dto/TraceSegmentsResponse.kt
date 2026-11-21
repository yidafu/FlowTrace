package dev.yidafu.flowtrace.query.dto

/**
 * `GET /v1/trace-segments` 响应（04 §3 / openapi `TraceSegmentsResponse`）。
 *
 * [nextCursor] 是 base64url 不透明串；为 null 表示已到末页。
 */
data class TraceSegmentsResponse(
    val segments: List<TraceSegment>,
    val nextCursor: String? = null,
)

/**
 * 单个轨迹段或停留点（openapi `TraceSegment`）。
 *
 * 字段取值约定：
 * - [kind]：`MOVE` | `STAY`
 * - [moveMode]：仅 MOVE 段有值，取值沿用领域 `MoveMode` 名称
 *   （`UNKNOWN`/`STILL`/`WALK`/`RUN`/`BICYCLE`/`VEHICLE`；openapi 示例枚举写的是 `BIKE`，
 *   `BICYCLE` ↔ `BIKE` 的口径尚未对齐，故这里用 String 而不锁死为枚举）
 * - [status]：`OPEN` | `FINALIZED`
 * - [geometry]：`format=wkt` 时为 WKT 原文，`format=geojson` 时为 GeoJSON 文本
 * - [stayLat] / [stayLng]：仅 STAY 段有值，MOVE 段为 null
 *
 * 坐标对外一律十进制度（库内 ×10⁷ 整数由组装层除回）；[durationSec] 由时间字段派生，
 * 避免响应里出现与 start/end 不一致的冗余值。
 */
data class TraceSegment(
    val segmentId: String,
    val kind: String,
    val startTsMs: Long,
    val endTsMs: Long,
    val pointCount: Int,
    val distanceM: Double,
    val geometry: String,
    val bbox: BoundingBox,
    val status: String,
    val moveMode: String? = null,
    val stayLat: Double? = null,
    val stayLng: Double? = null,
) {
    /** 段时长（秒）。31 天跨度内不会溢出 Int，这里取 Long 与毫秒时间戳同口径。 */
    val durationSec: Long
        get() = (endTsMs - startTsMs) / 1000
}

/** 段包围盒（openapi `BoundingBox`），字段顺序与 openapi 一致。 */
data class BoundingBox(
    val minLat: Double,
    val minLng: Double,
    val maxLat: Double,
    val maxLng: Double,
)
