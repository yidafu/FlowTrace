package dev.yidafu.flowtrace.query

import dev.yidafu.flowtrace.query.dto.DistanceResponse
import dev.yidafu.flowtrace.query.dto.PlaybackResponse
import dev.yidafu.flowtrace.query.dto.TracePointsCompactResponse
import dev.yidafu.flowtrace.query.dto.TracePointsGeojsonResponse
import dev.yidafu.flowtrace.query.dto.TraceSegmentsResponse
import dev.yidafu.flowtrace.query.service.DistanceQueryResult
import dev.yidafu.flowtrace.query.service.DistanceQueryService
import dev.yidafu.flowtrace.query.service.PlaybackQueryService
import dev.yidafu.flowtrace.query.service.SegmentQueryService
import dev.yidafu.flowtrace.query.service.TracePointPage
import dev.yidafu.flowtrace.query.service.TraceQueryService
import dev.yidafu.flowtrace.query.web.CursorCodec
import dev.yidafu.flowtrace.query.web.TraceSimplifier
import dev.yidafu.flowtrace.shared.model.SegmentKind
import dev.yidafu.flowtrace.shared.model.TracePoint
import dev.yidafu.flowtrace.shared.persistence.repository.TracePointCursor
import dev.yidafu.flowtrace.shared.persistence.repository.TraceSegmentCursor
import dev.yidafu.flowtrace.shared.web.ApiErrorResponse
import org.mockito.Mockito
import org.springframework.http.HttpStatus
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TraceQueryControllerTest {
    private val response = TraceSegmentsResponse(emptyList())
    private val point =
        TracePoint(
            tsMs = START.toEpochMilli(),
            pointId = UUID.fromString("9af0c1d2-0000-5000-8000-000000000001"),
            userId = "u-1",
            deviceId = "d-1",
            batchId = UUID.fromString("8af0c1d2-0000-5000-8000-000000000001"),
            latQ = 391_234_567,
            lngQ = 1_161_234_567,
        )
    private val pointPage = TracePointPage(listOf(point), originalPoints = 1, hasMore = true, nextCursor = "next-cursor")
    private val segmentQueryService =
        Mockito.mock(SegmentQueryService::class.java) { invocation ->
            if (invocation.method.name == "listSegments") response else Mockito.RETURNS_DEFAULTS.answer(invocation)
        }
    private val traceQueryService =
        Mockito.mock(TraceQueryService::class.java) { invocation ->
            if (invocation.method.name == "findByUser") pointPage else Mockito.RETURNS_DEFAULTS.answer(invocation)
        }
    private val playbackResponse =
        PlaybackResponse(
            userId = "u-1",
            start = START.toString(),
            end = END.toString(),
            zoom = 15,
            withAttrs = false,
            points = listOf(listOf(point.tsMs, point.latQ.toLong(), point.lngQ.toLong())),
            returnedPoints = 1,
            originalPoints = 1,
        )
    private val distanceResponse = DistanceResponse(100.0, mapOf("WALK" to 100.0), "segments")
    private val playbackQueryService = Mockito.mock(PlaybackQueryService::class.java) { playbackResponse }
    private val distanceQueryService =
        Mockito.mock(DistanceQueryService::class.java) { DistanceQueryResult(distanceResponse, false) }
    private val codec = CursorCodec()
    private val controller =
        TraceQueryController(
            traceQueryService,
            segmentQueryService,
            playbackQueryService,
            distanceQueryService,
            TraceSimplifier(),
            codec,
        )

    // ------------------------------------------------------------------ /v1/traces

    @Test
    fun `轨迹点参数非法返回 400 统一错误体`() {
        val cases =
            listOf(
                pointCall(start = END, end = START),
                pointCall(end = START.plusSeconds(8 * 86_400L)),
                pointCall(limit = 0),
                pointCall(limit = 5001),
                pointCall(maxPoints = 0),
                pointCall(zoom = -1),
                pointCall(zoom = 22),
                pointCall(format = "protobuf"),
                pointCall(cursor = "!!!"),
            )
        cases.forEach { result ->
            assertEquals(HttpStatus.BAD_REQUEST, result.statusCode)
            assertEquals("VALIDATION_ERROR", (result.body as ApiErrorResponse).error.code)
        }
    }

    @Test
    fun `轨迹点身份头串号返回 403`() {
        val result = pointCall(xUserId = "u-2")

        assertEquals(HttpStatus.FORBIDDEN, result.statusCode)
        assertEquals("FORBIDDEN", (result.body as ApiErrorResponse).error.code)
    }

    @Test
    fun `geojson 响应组装为 LineString 加平行时间戳`() {
        val result = pointCall()

        assertEquals(HttpStatus.OK, result.statusCode)
        val body = result.body as TracePointsGeojsonResponse
        assertEquals("u-1", body.userId)
        assertEquals(START.toString(), body.start)
        assertEquals(END.toString(), body.end)
        assertEquals("geojson", body.format)
        assertEquals(1, body.geometry.coordinates.size)
        assertEquals(point.lngQ / 1e7, body.geometry.coordinates[0][0], 1e-12)
        assertEquals(point.latQ / 1e7, body.geometry.coordinates[0][1], 1e-12)
        assertEquals(listOf(point.tsMs), body.timestampsMs)
        assertEquals(1, body.returnedPoints)
        assertEquals(1, body.originalPoints)
        assertEquals("next-cursor", body.nextCursor)
        assertTrue(body.hasMore)
    }

    @Test
    fun `compact 响应返回整数三元组`() {
        val result = pointCall(format = "compact")

        assertEquals(HttpStatus.OK, result.statusCode)
        val body = result.body as TracePointsCompactResponse
        assertEquals("compact", body.format)
        assertEquals(listOf(listOf(point.tsMs, point.latQ.toLong(), point.lngQ.toLong())), body.points)
        assertEquals(1, body.returnedPoints)
        assertEquals("next-cursor", body.nextCursor)
        assertTrue(body.hasMore)
    }

    @Test
    fun `不简化时把解码游标与 limit 原样交给服务`() {
        val cursor = TracePointCursor(point.tsMs, point.pointId)

        val result = pointCall(simplify = false, cursor = codec.encode(cursor), limit = 10)

        assertEquals(HttpStatus.OK, result.statusCode)
        val invocation = Mockito.mockingDetails(traceQueryService).invocations.last()
        assertEquals(cursor, invocation.getArgument<Any>(4))
        assertEquals(10, invocation.getArgument<Int>(3))
    }

    // ------------------------------------------------------------------ /v1/trace-segments

    @Test
    fun `参数非法返回 400 统一错误体`() {
        val cases =
            listOf(
                call(start = END, end = START),
                call(end = START.plusSeconds(32 * 86_400L)),
                call(kind = "run"),
                call(format = "protobuf"),
                call(limit = 0),
                call(cursor = "!!!"),
            )
        cases.forEach { result ->
            assertEquals(HttpStatus.BAD_REQUEST, result.statusCode)
            assertEquals("VALIDATION_ERROR", (result.body as ApiErrorResponse).error.code)
        }
    }

    @Test
    fun `身份头串号返回 403`() {
        val result = call(xUserId = "u-2")

        assertEquals(HttpStatus.FORBIDDEN, result.statusCode)
        assertEquals("FORBIDDEN", (result.body as ApiErrorResponse).error.code)
    }

    @Test
    fun `合法请求把解码后的游标与过滤条件交给服务`() {
        val cursor = TraceSegmentCursor(START.toEpochMilli(), UUID.fromString("9af0c1d2-0000-5000-8000-000000000001"))

        val result = call(kind = "stay", format = "geojson", limit = 10, cursor = codec.encode(cursor))

        assertEquals(HttpStatus.OK, result.statusCode)
        assertTrue(result.body === response)
        val invocation = Mockito.mockingDetails(segmentQueryService).invocations.last()
        // 这里看的是服务方法 invocation（9 个参数），不是控制器方法（10 个参数）。
        assertEquals(cursor, invocation.getArgument<Any>(8))
        assertEquals(SegmentKind.STAY, invocation.getArgument<Any>(3))
        assertEquals(10, invocation.getArgument<Int>(7))
    }

    // ------------------------------------------------------------------ /v1/traces/playback

    @Test
    fun `回放参数非法返回 400 统一错误体`() {
        val cases =
            listOf(
                playbackCall(start = END, end = START),
                playbackCall(end = START.plusSeconds(25 * 3_600L)),
                playbackCall(zoom = -1),
                playbackCall(zoom = 22),
                playbackCall(maxPoints = 0),
            )
        cases.forEach { result ->
            assertEquals(HttpStatus.BAD_REQUEST, result.statusCode)
            assertEquals("VALIDATION_ERROR", (result.body as ApiErrorResponse).error.code)
        }
    }

    @Test
    fun `回放身份头串号返回 403`() {
        val result = playbackCall(xUserId = "u-2")

        assertEquals(HttpStatus.FORBIDDEN, result.statusCode)
        assertEquals("FORBIDDEN", (result.body as ApiErrorResponse).error.code)
    }

    @Test
    fun `回放把参数交给服务并原样返回响应`() {
        val result = playbackCall(zoom = 17, maxPoints = 300, withAttrs = true)

        assertEquals(HttpStatus.OK, result.statusCode)
        assertTrue(result.body === playbackResponse)
        val invocation = Mockito.mockingDetails(playbackQueryService).invocations.last()
        assertEquals("u-1", invocation.getArgument<String>(0))
        assertEquals(17, invocation.getArgument<Int>(3))
        assertEquals(300, invocation.getArgument<Int>(4))
        assertEquals(true, invocation.getArgument<Boolean>(5))
    }

    // ------------------------------------------------------------------ /v1/traces/distance

    @Test
    fun `里程参数非法返回 400 统一错误体`() {
        val cases =
            listOf(
                distanceCall(start = END, end = START),
                distanceCall(end = START.plusSeconds(32 * 86_400L)),
            )
        cases.forEach { result ->
            assertEquals(HttpStatus.BAD_REQUEST, result.statusCode)
            assertEquals("VALIDATION_ERROR", (result.body as ApiErrorResponse).error.code)
        }
    }

    @Test
    fun `里程身份头串号返回 403`() {
        val result = distanceCall(xUserId = "u-2")

        assertEquals(HttpStatus.FORBIDDEN, result.statusCode)
        assertEquals("FORBIDDEN", (result.body as ApiErrorResponse).error.code)
    }

    @Test
    fun `里程正常口径不返回降级头`() {
        val result = distanceCall(byMode = false)

        assertEquals(HttpStatus.OK, result.statusCode)
        assertTrue(result.body === distanceResponse)
        assertEquals(null, result.headers.getFirst("X-FlowTrace-Degraded"))
        val invocation = Mockito.mockingDetails(distanceQueryService).invocations.last()
        assertEquals(false, invocation.getArgument<Boolean>(3))
    }

    @Test
    fun `里程降级口径返回降级头`() {
        val degraded =
            Mockito.mock(DistanceQueryService::class.java) {
                DistanceQueryResult(distanceResponse.copy(source = "points"), true)
            }
        val degradedController =
            TraceQueryController(traceQueryService, segmentQueryService, playbackQueryService, degraded, TraceSimplifier(), codec)

        val result = degradedController.distance(null, "u-1", START, END, true)

        assertEquals(HttpStatus.OK, result.statusCode)
        assertEquals("true", result.headers.getFirst("X-FlowTrace-Degraded"))
    }

    private fun pointCall(
        userId: String = "u-1",
        start: Instant = START,
        end: Instant = END,
        simplify: Boolean = true,
        zoom: Int = 15,
        maxPoints: Int = 2000,
        format: String = "geojson",
        limit: Int = 2000,
        cursor: String? = null,
        xUserId: String? = null,
    ) = controller.listTraces(xUserId, userId, start, end, simplify, zoom, maxPoints, format, limit, cursor)

    private fun call(
        userId: String = "u-1",
        start: Instant = START,
        end: Instant = END,
        kind: String = "all",
        format: String = "wkt",
        limit: Int = 500,
        cursor: String? = null,
        xUserId: String? = null,
    ) = controller.listSegments(xUserId, userId, start, end, kind, null, null, format, limit, cursor)

    private fun playbackCall(
        userId: String = "u-1",
        start: Instant = START,
        end: Instant = END,
        zoom: Int = 15,
        maxPoints: Int = 500,
        withAttrs: Boolean = false,
        xUserId: String? = null,
    ) = controller.playback(xUserId, userId, start, end, zoom, maxPoints, withAttrs)

    private fun distanceCall(
        userId: String = "u-1",
        start: Instant = START,
        end: Instant = END,
        byMode: Boolean = true,
        xUserId: String? = null,
    ) = controller.distance(xUserId, userId, start, end, byMode)

    private companion object {
        val START: Instant = Instant.parse("2026-11-01T00:00:00Z")
        val END: Instant = START.plusSeconds(3_600)
    }
}
