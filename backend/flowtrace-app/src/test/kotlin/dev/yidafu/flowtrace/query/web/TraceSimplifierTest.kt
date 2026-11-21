package dev.yidafu.flowtrace.query.web

import dev.yidafu.flowtrace.shared.model.TracePoint
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

class TraceSimplifierTest {
    private val simplifier = TraceSimplifier()

    @Test
    fun `缩放级别 LUT 与 06 节 5 2 一致`() {
        val expected =
            mapOf(
                0 to 100.0,
                10 to 100.0,
                11 to 50.0,
                12 to 50.0,
                13 to 20.0,
                14 to 20.0,
                15 to 10.0,
                16 to 10.0,
                17 to 3.0,
                21 to 3.0,
            )
        expected.forEach { (zoom, epsilon) -> assertEquals(epsilon, simplifier.lut(zoom), "zoom=$zoom") }
    }

    @Test
    fun `等距抽取数量正确且保首尾`() {
        val points = points(100)
        val sampled = simplifier.evenlySample(points, 5)

        assertEquals(5, sampled.size)
        assertEquals(points.first(), sampled.first())
        assertEquals(points.last(), sampled.last())
        assertEquals(listOf(points.first(), points.last()), simplifier.evenlySample(points, 1))
    }

    @Test
    fun `兜底抽样作用在 DP 结果而非原始点`() {
        // 相邻点仅差 1m，ε=100m 时 DP 只会保留首尾；若误对原始点抽样会得到 5 个点。
        val result = simplifier.simplify(points(30), zoom = 15, maxPoint = 5)

        assertEquals(2, result.size)
        assertEquals(points(30).first(), result.first())
        assertEquals(points(30).last(), result.last())
    }

    private fun points(count: Int): List<TracePoint> =
        (0 until count).map { i ->
            TracePoint(
                tsMs = Instant.parse("2026-11-01T00:00:00Z").toEpochMilli() + i * 1_000L,
                pointId = UUID.fromString("9af0c1d2-0000-5000-8000-${"%012d".format(i + 1)}"),
                userId = "u-1",
                deviceId = "d-1",
                batchId = UUID.fromString("8af0c1d2-0000-5000-8000-000000000001"),
                latQ = 391_234_560 + i,
                lngQ = 1_161_234_560 + i,
            )
        }
}
