package dev.yidafu.flowtrace.query.service

import dev.yidafu.flowtrace.query.web.CursorCodec
import dev.yidafu.flowtrace.shared.model.GeoMath
import dev.yidafu.flowtrace.shared.model.MoveMode
import dev.yidafu.flowtrace.shared.model.TracePoint
import dev.yidafu.flowtrace.shared.persistence.repository.TracePointRepository
import dev.yidafu.flowtrace.shared.persistence.repository.TraceSegmentRepository
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito
import org.mockito.Mockito.`when`
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DistanceQueryServiceTest {
    private val start = Instant.parse("2026-11-01T00:00:00Z")
    private val end = start.plusSeconds(3_600)
    private val from = start.toEpochMilli()
    private val to = end.toEpochMilli()
    private val segmentRepository = Mockito.mock(TraceSegmentRepository::class.java)
    private val pointRepository = Mockito.mock(TracePointRepository::class.java)
    private val service =
        DistanceQueryService(
            segmentRepository,
            pointRepository,
            TraceQueryService(pointRepository, CursorCodec()),
        )

    @Test
    fun `段口径按出行模式求和`() {
        `when`(segmentRepository.sumDistanceByMode("u-1", from, to))
            .thenReturn(mapOf(MoveMode.WALK to 100.0, MoveMode.BICYCLE to 50.0))
        `when`(segmentRepository.hasOpenInRange("u-1", from, to)).thenReturn(false)

        val result = service.distance("u-1", start, end, true)

        assertEquals(150.0, result.response.totalM, 1e-9)
        assertEquals(mapOf("WALK" to 100.0, "BICYCLE" to 50.0), result.response.byMode)
        assertEquals("segments", result.response.source)
        assertEquals(false, result.degraded)
    }

    @Test
    fun `段口径下 byMode 为 false 时省略分组`() {
        `when`(segmentRepository.sumDistanceByMode("u-1", from, to)).thenReturn(mapOf(MoveMode.WALK to 100.0))
        `when`(segmentRepository.hasOpenInRange("u-1", from, to)).thenReturn(false)

        val result = service.distance("u-1", start, end, false)

        assertEquals(100.0, result.response.totalM, 1e-9)
        assertTrue(result.response.byMode.isEmpty())
    }

    @Test
    fun `存在 OPEN 段时降级为点级里程`() {
        `when`(segmentRepository.sumDistanceByMode("u-1", from, to)).thenReturn(mapOf(MoveMode.WALK to 100.0))
        `when`(segmentRepository.hasOpenInRange("u-1", from, to)).thenReturn(true)
        val a = point(1_000, 391_234_567, 1_161_234_567)
        val b = point(61_000, 391_244_567, 1_161_244_567)
        stubPoints(listOf(a, b))

        val result = service.distance("u-1", start, end, true)

        assertEquals(GeoMath.haversineM(a.latQ, a.lngQ, b.latQ, b.lngQ), result.response.totalM, 1e-6)
        assertEquals("points", result.response.source)
        assertTrue(result.response.byMode.isEmpty())
        assertTrue(result.degraded)
    }

    @Test
    fun `有点无段时判定缺失段并降级`() {
        `when`(segmentRepository.sumDistanceByMode("u-1", from, to)).thenReturn(emptyMap())
        `when`(segmentRepository.hasOpenInRange("u-1", from, to)).thenReturn(false)
        `when`(pointRepository.countByUser("u-1", from, to)).thenReturn(2L)
        val a = point(1_000, 391_234_567, 1_161_234_567)
        val b = point(61_000, 391_244_567, 1_161_244_567)
        stubPoints(listOf(a, b))

        val result = service.distance("u-1", start, end, true)

        assertEquals(GeoMath.haversineM(a.latQ, a.lngQ, b.latQ, b.lngQ), result.response.totalM, 1e-6)
        assertEquals("points", result.response.source)
        assertTrue(result.degraded)
    }

    @Test
    fun `降级时不同设备的点不互相连成里程`() {
        `when`(segmentRepository.sumDistanceByMode("u-1", from, to)).thenReturn(emptyMap())
        `when`(segmentRepository.hasOpenInRange("u-1", from, to)).thenReturn(true)
        stubPoints(
            listOf(
                point(1_000, 391_234_567, 1_161_234_567, deviceId = "d-1"),
                point(61_000, 391_244_567, 1_161_244_567, deviceId = "d-2"),
            ),
        )

        val result = service.distance("u-1", start, end, true)

        assertEquals(0.0, result.response.totalM, 1e-9)
        assertTrue(result.degraded)
    }

    private fun stubPoints(points: List<TracePoint>) {
        `when`(pointRepository.findByUser(anyString(), anyLong(), anyLong(), anyInt(), Mockito.any())).thenReturn(points)
    }

    private fun point(
        tsMs: Long,
        latQ: Int,
        lngQ: Int,
        deviceId: String = "d-1",
    ) = TracePoint(
        tsMs = tsMs,
        pointId = UUID.randomUUID(),
        userId = "u-1",
        deviceId = deviceId,
        batchId = UUID.randomUUID(),
        latQ = latQ,
        lngQ = lngQ,
    )
}
