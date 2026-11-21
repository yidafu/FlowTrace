package dev.yidafu.flowtrace.query

import dev.yidafu.flowtrace.query.dto.LineString
import dev.yidafu.flowtrace.query.dto.TracePointsCompactResponse
import dev.yidafu.flowtrace.query.dto.TracePointsGeojsonResponse
import dev.yidafu.flowtrace.query.dto.TraceQueryFormat
import dev.yidafu.flowtrace.query.dto.TraceSegmentFormat
import dev.yidafu.flowtrace.query.service.DistanceQueryService
import dev.yidafu.flowtrace.query.service.PlaybackQueryService
import dev.yidafu.flowtrace.query.service.SegmentQueryService
import dev.yidafu.flowtrace.query.service.TraceQueryService
import dev.yidafu.flowtrace.query.web.CursorCodec
import dev.yidafu.flowtrace.query.web.InvalidCursorException
import dev.yidafu.flowtrace.query.web.TraceSimplifier
import dev.yidafu.flowtrace.shared.model.SegmentKind
import dev.yidafu.flowtrace.shared.persistence.repository.TracePointCursor
import dev.yidafu.flowtrace.shared.persistence.repository.TraceSegmentCursor
import dev.yidafu.flowtrace.shared.web.ApiErrorCode
import dev.yidafu.flowtrace.shared.web.apiError
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Instant
import java.util.concurrent.TimeUnit

/**
 * 轨迹查询控制器（docs/design/04-api.md §3）：轨迹点查询、轨迹段/停留点、回放、里程。
 *
 * 通用约定：时间参数 ISO-8601 UTC，end 独占；分页游标式（cursor/nextCursor）；
 * 坐标空间计算（抽稀/里程）全部在应用层 JTS 完成，不做数据库地理查询。
 */
@RestController
class TraceQueryController(
    private val traceQueryService: TraceQueryService,
    private val segmentQueryService: SegmentQueryService,
    private val playbackQueryService: PlaybackQueryService,
    private val distanceQueryService: DistanceQueryService,
    private val simplifier: TraceSimplifier,
    private val codec: CursorCodec,
) {
    /**
     * 轨迹点查询：GET /v1/traces（默认简化轨迹）。
     *
     * [simplify] 默认 true；false 时忽略 zoom/maxPoints 返回原始点（仍受 limit 分页约束）。
     * [zoom] 0..21，默认 15，简化 LUT 见 06 §5.2。
     * [maxPoints] 默认 2000，简化后仍超则等距抽取。
     * [format] geojson|compact，默认 geojson（compact 为 [tsMs, latE7, lngE7] 整数三元组）。
     * [limit] 默认 2000，上限 5000。跨度约束：end-start ≤ 7 天。
     *
     * 校验顺序：身份一致性 → 时间区间/跨度 → limit/maxPoints/zoom → 枚举 → 游标；
     * 参数错误统一走 400 `VALIDATION_ERROR`（04 §1.4）。
     */
    @GetMapping("/v1/traces")
    fun listTraces(
        @RequestHeader("X-User-Id", required = false) xUserId: String?,
        @RequestParam("userId") userId: String,
        @RequestParam("start") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) start: Instant,
        @RequestParam("end") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) end: Instant,
        @RequestParam("simplify", defaultValue = "true") simplify: Boolean,
        @RequestParam("zoom", defaultValue = "15") zoom: Int,
        @RequestParam("maxPoints", defaultValue = "2000") maxPoints: Int,
        @RequestParam("format", defaultValue = "geojson") format: String,
        @RequestParam("limit", defaultValue = "2000") limit: Int,
        @RequestParam("cursor", required = false) cursor: String?,
    ): ResponseEntity<Any> {
        if (xUserId != null && xUserId != userId) return forbidden()
        if (start >= end) {
            return badRequest("start 必须早于 end")
        }
        if (end.toEpochMilli() - start.toEpochMilli() > TimeUnit.DAYS.toMillis(MAX_TRACE_SPAN_DAYS)) {
            return badRequest("查询跨度不得超过 $MAX_TRACE_SPAN_DAYS 天")
        }
        if (limit !in 1..MAX_TRACE_LIMIT) {
            return badRequest("limit 必须在 1..$MAX_TRACE_LIMIT 之间")
        }
        if (maxPoints <= 0) {
            return badRequest("maxPoints 必须大于 0")
        }
        if (zoom !in 0..21) {
            return badRequest("zoom 必须在 0..21 之间")
        }
        val outputFormat = TraceQueryFormat.fromValue(format.lowercase()) ?: return badRequest("format 必须是 geojson 或 compact")
        val pointCursor =
            if (cursor != null) {
                try {
                    codec.decode(cursor, TracePointCursor::class.java)
                } catch (ex: InvalidCursorException) {
                    return badRequest(ex.message ?: "非法分页游标")
                }
            } else {
                null
            }

        val page = traceQueryService.findByUser(userId, start, end, limit, pointCursor)
        val outputPoints = if (simplify) simplifier.simplify(page.points, zoom, maxPoints) else page.points
        val startIso = start.toString()
        val endIso = end.toString()
        if (outputFormat == TraceQueryFormat.GEOJSON) {
            return ResponseEntity.ok(
                TracePointsGeojsonResponse(
                    userId = userId,
                    start = startIso,
                    end = endIso,
                    simplify = simplify,
                    zoom = zoom,
                    format = TraceQueryFormat.GEOJSON.value,
                    geometry =
                        LineString(
                            coordinates =
                                outputPoints.map { listOf(it.lngQ / COORD_SCALE, it.latQ / COORD_SCALE) },
                        ),
                    timestampsMs = outputPoints.map { it.tsMs },
                    returnedPoints = outputPoints.size,
                    originalPoints = page.originalPoints,
                    nextCursor = page.nextCursor,
                    hasMore = page.hasMore,
                ),
            )
        }
        return ResponseEntity.ok(
            TracePointsCompactResponse(
                userId = userId,
                start = startIso,
                end = endIso,
                simplify = simplify,
                zoom = zoom,
                format = TraceQueryFormat.COMPACT.value,
                points =
                    outputPoints.map { p ->
                        listOf(p.tsMs, p.latQ.toLong(), p.lngQ.toLong())
                    },
                returnedPoints = outputPoints.size,
                originalPoints = page.originalPoints,
                nextCursor = page.nextCursor,
                hasMore = page.hasMore,
            ),
        )
    }

    /**
     * 轨迹段/停留点查询：GET /v1/trace-segments。跨度约束：end-start ≤ 31 天。
     *
     * [kind] move|stay|all，默认 all。
     * [sessionId]/[taskId] 巡检业务关联过滤（业务经此关联轨迹，不侵入上报主链路）。
     * [format] wkt|geojson，默认 wkt。
     *
     * 校验顺序：身份一致性 → 时间区间/跨度 → limit → 枚举 → 游标。参数错误统一 400
     * `VALIDATION_ERROR`（04 §1.4），不把 Jackson 的游标解析异常冒泡成 500。
     */
    @GetMapping("/v1/trace-segments")
    fun listSegments(
        @RequestHeader("X-User-Id", required = false) xUserId: String?,
        @RequestParam("userId") userId: String,
        @RequestParam("start") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) start: Instant,
        @RequestParam("end") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) end: Instant,
        @RequestParam("kind", defaultValue = "all") kind: String,
        @RequestParam("sessionId", required = false) sessionId: String?,
        @RequestParam("taskId", required = false) taskId: String?,
        @RequestParam("format", defaultValue = "wkt") format: String,
        @RequestParam("limit", defaultValue = "500") limit: Int,
        @RequestParam("cursor", required = false) cursor: String?,
    ): ResponseEntity<Any> {
        // 网关注入 X-User-Id；缺失时按本地开发环境放行，存在但不一致即拒绝串号。
        if (xUserId != null && xUserId != userId) return forbidden()
        if (start >= end) {
            return badRequest("start 必须早于 end")
        }
        if (end.toEpochMilli() - start.toEpochMilli() > TimeUnit.DAYS.toMillis(MAX_SEGMENT_SPAN_DAYS)) {
            return badRequest("查询跨度不得超过 $MAX_SEGMENT_SPAN_DAYS 天")
        }
        if (limit !in 1..MAX_SEGMENT_LIMIT) {
            return badRequest("limit 必须在 1..$MAX_SEGMENT_LIMIT 之间")
        }
        val segmentKind =
            when (kind.lowercase()) {
                "all" -> null
                "move" -> SegmentKind.MOVE
                "stay" -> SegmentKind.STAY
                else -> return badRequest("kind 必须是 move、stay 或 all")
            }
        val outputFormat = TraceSegmentFormat.fromValue(format.lowercase()) ?: return badRequest("format 必须是 wkt 或 geojson")
        val segmentCursor =
            if (cursor != null) {
                try {
                    codec.decode(cursor, TraceSegmentCursor::class.java)
                } catch (ex: InvalidCursorException) {
                    return badRequest(ex.message ?: "非法分页游标")
                }
            } else {
                null
            }
        return ResponseEntity.ok(
            segmentQueryService.listSegments(
                userId = userId,
                start = start,
                end = end,
                kind = segmentKind,
                sessionId = sessionId,
                taskId = taskId,
                format = outputFormat,
                limit = limit,
                cursor = segmentCursor,
            ),
        )
    }

    /**
     * 轨迹回放：GET /v1/traces/playback（按缩放级别取点）。跨度约束：end-start ≤ 24h。
     *
     * 等价于 /v1/traces 简化管道，固定返回 compact 数组；
     * [withAttrs] true 时每点附 speed/bearing 可选字段。
     */
    @GetMapping("/v1/traces/playback")
    fun playback(
        @RequestHeader("X-User-Id", required = false) xUserId: String?,
        @RequestParam("userId") userId: String,
        @RequestParam("start") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) start: Instant,
        @RequestParam("end") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) end: Instant,
        @RequestParam("zoom", defaultValue = "15") zoom: Int,
        @RequestParam("maxPoints", defaultValue = "500") maxPoints: Int,
        @RequestParam("withAttrs", defaultValue = "false") withAttrs: Boolean,
    ): ResponseEntity<Any> {
        if (xUserId != null && xUserId != userId) return forbidden()
        if (start >= end) {
            return badRequest("start 必须早于 end")
        }
        if (end.toEpochMilli() - start.toEpochMilli() > TimeUnit.DAYS.toMillis(MAX_PLAYBACK_SPAN_DAYS)) {
            return badRequest("查询跨度不得超过 $MAX_PLAYBACK_SPAN_DAYS 天")
        }
        if (zoom !in 0..21) {
            return badRequest("zoom 必须在 0..21 之间")
        }
        if (maxPoints <= 0) {
            return badRequest("maxPoints 必须大于 0")
        }
        return ResponseEntity.ok(
            playbackQueryService.playback(userId, start, end, zoom, maxPoints, withAttrs),
        )
    }

    /**
     * 里程计算：GET /v1/traces/distance。跨度约束：end-start ≤ 31 天。
     *
     * [byMode] 默认 true，按出行模式分组。口径：段表求和（kind=MOVE）；
     * 范围内存在 OPEN 段或缺失段时回退原始点逐段 haversine（source="points"，
     * 响应头 X-FlowTrace-Degraded: true）。
     */
    @GetMapping("/v1/traces/distance")
    fun distance(
        @RequestHeader("X-User-Id", required = false) xUserId: String?,
        @RequestParam("userId") userId: String,
        @RequestParam("start") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) start: Instant,
        @RequestParam("end") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) end: Instant,
        @RequestParam("byMode", defaultValue = "true") byMode: Boolean,
    ): ResponseEntity<Any> {
        if (xUserId != null && xUserId != userId) return forbidden()
        if (start >= end) {
            return badRequest("start 必须早于 end")
        }
        if (end.toEpochMilli() - start.toEpochMilli() > TimeUnit.DAYS.toMillis(MAX_DISTANCE_SPAN_DAYS)) {
            return badRequest("查询跨度不得超过 $MAX_DISTANCE_SPAN_DAYS 天")
        }
        val result = distanceQueryService.distance(userId, start, end, byMode)
        return if (result.degraded) {
            ResponseEntity.ok().header(DEGRADED_HEADER, "true").body(result.response)
        } else {
            ResponseEntity.ok(result.response)
        }
    }

    private fun badRequest(message: String): ResponseEntity<Any> =
        ResponseEntity.badRequest().body(apiError(ApiErrorCode.VALIDATION_ERROR, message))

    private fun forbidden(): ResponseEntity<Any> =
        ResponseEntity.status(HttpStatus.FORBIDDEN).body(apiError(ApiErrorCode.FORBIDDEN, "X-User-Id 与 userId 不一致"))

    private companion object {
        const val MAX_TRACE_SPAN_DAYS = 7L
        const val MAX_TRACE_LIMIT = 5000
        const val COORD_SCALE = 1e7
        const val MAX_SEGMENT_SPAN_DAYS = 31L
        const val MAX_PLAYBACK_SPAN_DAYS = 1L
        const val MAX_DISTANCE_SPAN_DAYS = 31L
        const val DEGRADED_HEADER = "X-FlowTrace-Degraded"

        // 文档只写了默认 500；WKT 单段 1–5KB，1000 段已到数 MB，设保守上限防止单响应过大。
        const val MAX_SEGMENT_LIMIT = 1000
    }
}
