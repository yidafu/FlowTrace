package dev.yidafu.flowtrace.query.service

import dev.yidafu.flowtrace.query.dto.BoundingBox
import dev.yidafu.flowtrace.query.dto.TraceSegment
import dev.yidafu.flowtrace.query.dto.TraceSegmentFormat
import dev.yidafu.flowtrace.query.dto.TraceSegmentsResponse
import dev.yidafu.flowtrace.query.web.CursorCodec
import dev.yidafu.flowtrace.shared.model.SegmentKind
import dev.yidafu.flowtrace.shared.persistence.repository.TraceSegmentCursor
import dev.yidafu.flowtrace.shared.persistence.repository.TraceSegmentRepository
import dev.yidafu.flowtrace.shared.persistence.repository.TraceSegmentRow
import org.locationtech.jts.io.WKTReader
import org.springframework.stereotype.Service
import java.time.Instant

/**
 * 轨迹段查询（06 §5.1）：把仓储行映射为 API 响应，并负责 keyset 分页的 `nextCursor`。
 *
 * 时间过滤、kind/session/task 过滤与 `(start_ts, segment_id)` 排序都在仓储层完成；
 * 本类只做「多取一条判 hasMore → 组装响应」，不解析业务语义。
 */
@Service
class SegmentQueryService(
    private val repository: TraceSegmentRepository,
    private val codec: CursorCodec,
) {
    fun listSegments(
        userId: String,
        start: Instant,
        end: Instant,
        kind: SegmentKind?,
        sessionId: String?,
        taskId: String?,
        format: TraceSegmentFormat,
        limit: Int,
        cursor: TraceSegmentCursor?,
    ): TraceSegmentsResponse {
        // 多取一条：只有看到第 limit+1 条才能确定还有下一页，避免额外 count 查询。
        val rows =
            repository.listByUser(
                userId = userId,
                fromMsInclusive = start.toEpochMilli(),
                toMsExclusive = end.toEpochMilli(),
                kind = kind,
                sessionId = sessionId,
                taskId = taskId,
                limit = limit + 1,
                after = cursor,
            )
        val hasMore = rows.size > limit
        val page = if (hasMore) rows.take(limit) else rows
        val nextCursor =
            if (hasMore) {
                page.lastOrNull()?.let { codec.encode(TraceSegmentCursor(it.startTsMs, it.segmentId)) }
            } else {
                null
            }
        return TraceSegmentsResponse(
            segments = page.map { it.toResponse(format) },
            nextCursor = nextCursor,
        )
    }

    private fun TraceSegmentRow.toResponse(format: TraceSegmentFormat): TraceSegment =
        TraceSegment(
            segmentId = segmentId.toString(),
            kind = kind.name,
            startTsMs = startTsMs,
            endTsMs = endTsMs,
            pointCount = pointCount,
            distanceM = distanceM,
            geometry = SegmentGeometry.output(geometryWkt, format),
            // bbox / 质心取自冗余整数列，除以 1e7 还原十进制度；不解析 WKT。
            bbox =
                BoundingBox(
                    minLat = bbox.minLatQ / COORD_SCALE,
                    minLng = bbox.minLngQ / COORD_SCALE,
                    maxLat = bbox.maxLatQ / COORD_SCALE,
                    maxLng = bbox.maxLngQ / COORD_SCALE,
                ),
            status = status.name,
            moveMode = if (kind == SegmentKind.MOVE) moveMode.name else null,
            stayLat = stayLatQ?.let { it / COORD_SCALE },
            stayLng = stayLngQ?.let { it / COORD_SCALE },
        )

    /** WKT → GeoJSON 文本：段几何由 processor 生成，只可能是 POINT / LINESTRING。 */
    private object SegmentGeometry {
        fun output(
            wkt: String,
            format: TraceSegmentFormat,
        ): String = if (format == TraceSegmentFormat.WKT) wkt else toGeoJson(wkt)

        private fun toGeoJson(wkt: String): String {
            val geometry = WKTReader().read(wkt)
            val coordinates = geometry.coordinates
            return when (geometry.geometryType) {
                "Point" -> {
                    require(coordinates.isNotEmpty()) { "POINT 几何缺少坐标: $wkt" }
                    """{"type":"Point","coordinates":[${coordinates[0].x},${coordinates[0].y}]}"""
                }
                "LineString" -> {
                    val body = coordinates.joinToString(",") { "[${it.x},${it.y}]" }
                    """{"type":"LineString","coordinates":[$body]}"""
                }
                else -> throw IllegalArgumentException("不支持的段几何类型: ${geometry.geometryType}")
            }
        }
    }

    private companion object {
        const val COORD_SCALE = 1e7
    }
}
