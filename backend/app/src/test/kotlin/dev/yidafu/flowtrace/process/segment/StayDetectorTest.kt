package dev.yidafu.flowtrace.process.segment

import dev.yidafu.flowtrace.shared.model.TracePoint
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class StayDetectorTest {
    private val detector = StayDetector()

    private fun detect(
        points: List<TracePoint>,
        radiusM: Double = 50.0,
        minMs: Long = 300_000,
        mergeGapMs: Long = 120_000,
    ) = detector.detect(points, radiusM, minMs, mergeGapMs)

    @Test
    fun `不足最短时长不产出 STAY`() {
        val points = listOf(TestPoints.of(0), TestPoints.of(60_000), TestPoints.of(120_000))
        assertTrue(detect(points).isEmpty())
    }

    @Test
    fun `满时长且在锚点半径内产出 STAY`() {
        val points = listOf(TestPoints.of(0), TestPoints.of(100_000), TestPoints.of(300_000))
        assertEquals(listOf(StayDetector.Stay(0, 2)), detect(points))
    }

    @Test
    fun `半径以锚点为准 不是以上一点为准`() {
        val east = TestPoints.lngQOfMeters(49.0)
        val west = TestPoints.lngQOfMeters(-49.0)
        // east → west 相邻距离约 98m > 50m，但两点距锚点都 < 50m
        val points =
            listOf(
                TestPoints.of(0),
                TestPoints.of(100_000, lngQ = east),
                TestPoints.of(200_000, lngQ = west),
                TestPoints.of(300_000),
            )
        assertEquals(listOf(StayDetector.Stay(0, 3)), detect(points))
    }

    @Test
    fun `超出锚点半径的点不进入 STAY`() {
        val far = TestPoints.lngQOfMeters(60.0)
        val points = listOf(TestPoints.of(0), TestPoints.of(100_000, lngQ = far), TestPoints.of(300_000))
        assertTrue(detect(points).isEmpty())
    }

    @Test
    fun `相邻 STAY 间隔不超过合并阈值时合并`() {
        val points =
            listOf(
                TestPoints.of(0),
                TestPoints.of(300_000),
                TestPoints.of(420_000),
                TestPoints.of(720_000),
            )
        assertEquals(listOf(StayDetector.Stay(0, 3)), detect(points))
    }

    @Test
    fun `合并后簇超出半径则不合并`() {
        val far = TestPoints.lngQOfMeters(80.0)
        val points =
            listOf(
                TestPoints.of(0),
                TestPoints.of(300_000),
                TestPoints.of(420_000, lngQ = far),
                TestPoints.of(720_000, lngQ = far),
            )
        assertEquals(listOf(StayDetector.Stay(0, 1), StayDetector.Stay(2, 3)), detect(points))
    }

    @Test
    fun `单点输入直接返回空`() {
        assertTrue(detect(listOf(TestPoints.of(0))).isEmpty())
    }
}
