package dev.yidafu.flowtrace.query.service

import dev.yidafu.flowtrace.query.dto.TraceSegmentFormat
import dev.yidafu.flowtrace.query.web.CursorCodec
import dev.yidafu.flowtrace.shared.model.Bbox
import dev.yidafu.flowtrace.shared.model.MoveMode
import dev.yidafu.flowtrace.shared.model.SegmentKind
import dev.yidafu.flowtrace.shared.model.SegmentStatus
import dev.yidafu.flowtrace.shared.persistence.repository.TraceSegmentCursor
import dev.yidafu.flowtrace.shared.persistence.repository.TraceSegmentRepository
import dev.yidafu.flowtrace.shared.persistence.repository.TraceSegmentRow
import org.mockito.Mockito
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SegmentQueryServiceTest {
    private val rows = (1..3).map { index -> row(index) }
    private val repository =
        Mockito.mock(TraceSegmentRepository::class.java) { invocation ->
            if (invocation.method.name == "listByUser") rows else Mockito.RETURNS_DEFAULTS.answer(invocation)
        }
    private val codec = CursorCodec()
    private val service = SegmentQueryService(repository, codec)

    @Test
    fun `多取一条判 hasMore 且游标指向本页最后一段`() {
        val response =
            service.listSegments(
                userId = "u-1",
                start = Instant.ofEpochMilli(START_MS),
                end = Instant.ofEpochMilli(END_MS),
                kind = SegmentKind.MOVE,
                sessionId = null,
                taskId = null,
                format = TraceSegmentFormat.WKT,
                limit = 2,
                cursor = null,
            )

        assertEquals(2, response.segments.size)
        val nextCursor = assertNotNull(response.nextCursor)
        assertEquals(TraceSegmentCursor(rows[1].startTsMs, rows[1].segmentId), codec.decode(nextCursor, TraceSegmentCursor::class.java))
        assertEquals(rows[0].segmentId.toString(), response.segments[0].segmentId)
        assertEquals("MOVE", response.segments[0].kind)
        assertEquals("WALK", response.segments[0].moveMode)
        assertEquals(3_900_000L / 10_000_000.0, response.segments[0].bbox.minLat, 0.0000001)
        assertTrue(response.segments[0].geometry.startsWith("LINESTRING"))
    }

    @Test
    fun `最后一页没有 nextCursor 且 geojson 分支转换 WKT`() {
        val response =
            service.listSegments(
                userId = "u-1",
                start = Instant.ofEpochMilli(START_MS),
                end = Instant.ofEpochMilli(END_MS),
                kind = null,
                sessionId = null,
                taskId = null,
                format = TraceSegmentFormat.GEOJSON,
                limit = 10,
                cursor = null,
            )

        assertEquals(3, response.segments.size)
        assertNull(response.nextCursor)
        assertTrue(response.segments[0].geometry.startsWith("{\"type\":\"LineString\",\"coordinates\":[[116.0,39.0]"))
    }

    private fun row(index: Int) =
        TraceSegmentRow(
            segmentId = UUID(0, index.toLong()),
            userId = "u-1",
            deviceId = "dev-1",
            sessionId = null,
            taskId = null,
            startTsMs = START_MS + index * 1_000L,
            endTsMs = START_MS + index * 1_000L + 500L,
            kind = SegmentKind.MOVE,
            moveMode = MoveMode.WALK,
            pointCount = 10,
            distanceM = 123.5,
            geometryWkt = "LINESTRING(116.0 39.0, 116.1 39.1)",
            bbox = Bbox(minLatQ = 3_900_000, maxLatQ = 3_910_000, minLngQ = 1_160_000_000, maxLngQ = 1_161_000_000),
            status = SegmentStatus.FINALIZED,
            updatedAt = Instant.ofEpochMilli(END_MS),
        )

    private companion object {
        const val START_MS = 1_763_376_000_000L
        const val END_MS = START_MS + 86_400_000L
    }
}
