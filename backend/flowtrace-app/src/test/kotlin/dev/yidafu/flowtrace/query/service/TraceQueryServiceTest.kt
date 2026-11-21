package dev.yidafu.flowtrace.query.service

import dev.yidafu.flowtrace.query.web.CursorCodec
import dev.yidafu.flowtrace.shared.model.TracePoint
import dev.yidafu.flowtrace.shared.persistence.repository.TracePointCursor
import dev.yidafu.flowtrace.shared.persistence.repository.TracePointRepository
import org.mockito.Mockito
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TraceQueryServiceTest {
    private var rows: List<TracePoint> = emptyList()
    private val repository =
        Mockito.mock(TracePointRepository::class.java) { invocation ->
            if (invocation.method.name == "findByUser") rows else Mockito.RETURNS_DEFAULTS.answer(invocation)
        }
    private val codec = CursorCodec()
    private val service = TraceQueryService(repository, codec)

    @Test
    fun `多取一条判分页并用本页最后一点生成游标`() {
        rows = points(3)
        val page = service.findByUser("u-1", START, END, limit = 2, cursor = null)

        assertTrue(page.hasMore)
        assertEquals(2, page.points.size)
        assertEquals(2, page.originalPoints)
        val cursor = codec.decode(page.nextCursor!!, TracePointCursor::class.java)
        assertEquals(rows[1].tsMs, cursor.tsMs)
        assertEquals(rows[1].pointId, cursor.pointId)
        // 仓储收到的是 limit+1，不是控制器传进来的 limit。
        val invocation = Mockito.mockingDetails(repository).invocations.last()
        assertEquals(3, invocation.getArgument<Int>(3))
    }

    @Test
    fun `不足一页时没有下一页也没有游标`() {
        rows = points(2)
        val page = service.findByUser("u-1", START, END, limit = 2, cursor = null)

        assertFalse(page.hasMore)
        assertNull(page.nextCursor)
        assertEquals(2, page.originalPoints)
    }

    @Test
    fun `传入游标原样交给仓储`() {
        rows = points(1)
        val cursor = TracePointCursor(rows[0].tsMs, rows[0].pointId)
        service.findByUser("u-1", START, END, limit = 2, cursor = cursor)

        val invocation = Mockito.mockingDetails(repository).invocations.last()
        assertEquals(cursor, invocation.getArgument<Any>(4))
        assertEquals(START.toEpochMilli(), invocation.getArgument<Long>(1))
        assertEquals(END.toEpochMilli(), invocation.getArgument<Long>(2))
    }

    private fun points(count: Int): List<TracePoint> =
        (0 until count).map { i ->
            TracePoint(
                tsMs = START.toEpochMilli() + i * 1_000L,
                pointId = UUID.fromString("9af0c1d2-0000-5000-8000-${"%012d".format(i + 1)}"),
                userId = "u-1",
                deviceId = "d-1",
                batchId = UUID.fromString("8af0c1d2-0000-5000-8000-000000000001"),
                latQ = 39_123_456 + i,
                lngQ = 116_123_456 + i,
            )
        }

    private companion object {
        val START: Instant = Instant.parse("2026-11-01T00:00:00Z")
        val END: Instant = START.plusSeconds(3_600)
    }
}
