package dev.yidafu.flowtrace.query.service

import dev.yidafu.flowtrace.query.dto.PlaybackResponse
import dev.yidafu.flowtrace.query.web.CursorCodec
import dev.yidafu.flowtrace.query.web.TraceSimplifier
import dev.yidafu.flowtrace.shared.model.TracePoint
import dev.yidafu.flowtrace.shared.persistence.repository.TracePointRepository
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito
import org.mockito.Mockito.`when`
import tools.jackson.databind.json.JsonMapper
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PlaybackQueryServiceTest {
    private val start = Instant.parse("2026-11-01T00:00:00Z")
    private val end = start.plusSeconds(3_600)
    private val repository = Mockito.mock(TracePointRepository::class.java)
    private val service =
        PlaybackQueryService(
            TraceQueryService(repository, CursorCodec()),
            repository,
            TraceSimplifier(),
        )

    @Test
    fun `回放按整窗点数与简化结果组装 compact 三元组`() {
        // 折线中间点明显偏离首尾连线，DP(10m) 会保留三点。
        val points = listOf(point(0), point(60_000, 391_244_567, 1_161_244_567), point(120_000, 391_234_567, 1_161_246_567))
        stubRead(points)
        `when`(repository.countByUser("u-1", start.toEpochMilli(), end.toEpochMilli())).thenReturn(3L)

        val response = service.playback("u-1", start, end, 15, 500, false)

        assertEquals(3, response.originalPoints)
        assertEquals(3, response.returnedPoints)
        assertEquals(false, response.withAttrs)
        assertEquals(listOf<Number>(1_000L, 391_234_567L, 1_161_234_567L), response.points.first())
        assertEquals(3, response.points.first().size)
    }

    @Test
    fun `带属性时每点追加速度与航向且缺省补零`() {
        stubRead(listOf(point(0, speedMps = 1.3f, bearingDeg = 45)))
        `when`(repository.countByUser("u-1", start.toEpochMilli(), end.toEpochMilli())).thenReturn(1L)

        val response = service.playback("u-1", start, end, 17, 500, true)

        assertEquals(true, response.withAttrs)
        assertEquals(listOf<Number>(1_000L, 391_234_567L, 1_161_234_567L, 1.3f, 45.toShort()), response.points.first())
        assertEquals(5, response.points.first().size)
    }

    @Test
    fun `点数超过上限时等距抽取并保首尾`() {
        val points =
            listOf(
                point(0),
                point(60_000, 391_244_567, 1_161_244_567),
                point(120_000, 391_234_567, 1_161_246_567),
                point(180_000, 391_240_000, 1_161_260_000),
            )
        stubRead(points)
        `when`(repository.countByUser("u-1", start.toEpochMilli(), end.toEpochMilli())).thenReturn(4L)

        val response = service.playback("u-1", start, end, 15, 2, false)

        assertEquals(4, response.originalPoints)
        assertEquals(2, response.returnedPoints)
        assertEquals(1_000L, response.points.first()[0])
        assertEquals(181_000L, response.points.last()[0])
    }

    @Test
    fun `回放点列序列化保留整数时间戳与量化坐标`() {
        val mapper = JsonMapper.builder().build()
        val response =
            PlaybackResponse(
                userId = "u-1",
                start = start.toString(),
                end = end.toString(),
                zoom = 15,
                withAttrs = true,
                points = listOf(listOf<Number>(1_000L, 391_234_567L, 1_161_234_567L, 1.3f, 45.toShort())),
                returnedPoints = 1,
                originalPoints = 1,
            )

        val json = mapper.writeValueAsString(response)

        assertTrue(json.contains("\"points\":[[1000,391234567,1161234567,1.3,45]]"), json)
    }

    private fun stubRead(points: List<TracePoint>) {
        `when`(repository.findByUser(anyString(), anyLong(), anyLong(), anyInt(), Mockito.any())).thenReturn(points)
    }

    private fun point(
        offsetMs: Long,
        latQ: Int = 391_234_567,
        lngQ: Int = 1_161_234_567,
        speedMps: Float? = null,
        bearingDeg: Short? = null,
    ) = TracePoint(
        tsMs = 1_000L + offsetMs,
        pointId = UUID.randomUUID(),
        userId = "u-1",
        deviceId = "d-1",
        batchId = UUID.randomUUID(),
        latQ = latQ,
        lngQ = lngQ,
        speedMps = speedMps,
        bearingDeg = bearingDeg,
    )
}
