package dev.yidafu.flowtrace.process.segment

import dev.yidafu.flowtrace.shared.model.MoveMode
import dev.yidafu.flowtrace.shared.model.PointFlags
import kotlin.test.Test
import kotlin.test.assertEquals

class MoveModeClassifierTest {
    private val classifier = MoveModeClassifier()

    @Test
    fun `速度阈值按 06 节 3 4 分档`() {
        assertEquals(MoveMode.STILL, classifier.fromSpeed(0.49f))
        assertEquals(MoveMode.WALK, classifier.fromSpeed(0.5f))
        assertEquals(MoveMode.WALK, classifier.fromSpeed(2.99f))
        assertEquals(MoveMode.RUN, classifier.fromSpeed(3f))
        assertEquals(MoveMode.RUN, classifier.fromSpeed(7.99f))
        assertEquals(MoveMode.BICYCLE, classifier.fromSpeed(8f))
        assertEquals(MoveMode.BICYCLE, classifier.fromSpeed(14.99f))
        assertEquals(MoveMode.VEHICLE, classifier.fromSpeed(15f))
        assertEquals(MoveMode.VEHICLE, classifier.fromSpeed(30f))
    }

    @Test
    fun `缺省与非法速度归 UNKNOWN`() {
        assertEquals(MoveMode.UNKNOWN, classifier.fromSpeed(Float.NaN))
        assertEquals(MoveMode.UNKNOWN, classifier.fromSpeed(Float.POSITIVE_INFINITY))
        assertEquals(MoveMode.UNKNOWN, classifier.fromSpeed(-1f))
        assertEquals(MoveMode.UNKNOWN, classifier.infer(TestPoints.of(0)))
    }

    @Test
    fun `有点速度时采信推断值 否则沿用 fallback`() {
        val walking = TestPoints.of(0, speedMps = 1.2f, flags = PointFlags.MOTION_CHANGE)
        val noSpeed = TestPoints.of(1)

        assertEquals(MoveMode.WALK, classifier.confirmedMode(walking, MoveMode.VEHICLE))
        assertEquals(MoveMode.VEHICLE, classifier.confirmedMode(noSpeed, MoveMode.VEHICLE))
        assertEquals(MoveMode.UNKNOWN, classifier.confirmedMode(noSpeed))
    }

    @Test
    fun `多数投票忽略 UNKNOWN 且平票确定性`() {
        val points =
            listOf(
                TestPoints.of(0, speedMps = 1f),
                TestPoints.of(1, speedMps = 1.2f),
                TestPoints.of(2, speedMps = 10f),
                TestPoints.of(3, speedMps = 12f),
                TestPoints.of(4),
            )

        // WALK 与 VEHICLE 各 2 票，首次达到 2 票的是 WALK
        assertEquals(MoveMode.WALK, classifier.majority(points))
        assertEquals(MoveMode.UNKNOWN, classifier.majority(listOf(TestPoints.of(0))))
    }
}
