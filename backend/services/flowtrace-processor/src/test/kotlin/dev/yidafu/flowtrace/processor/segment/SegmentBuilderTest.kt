package dev.yidafu.flowtrace.processor.segment

import dev.yidafu.flowtrace.model.GeoMath
import dev.yidafu.flowtrace.model.Ids
import dev.yidafu.flowtrace.model.SegmentKind
import dev.yidafu.flowtrace.model.TracePoint
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SegmentBuilderTest {
    private val builder = SegmentBuilder()

    private fun build(
        points: List<TracePoint>,
        sessionId: String? = null,
    ) = builder.build(
        userId = "u-1",
        sessionId = sessionId,
        points = points,
        epsilonM = 5.0,
        stayRadiusM = 50.0,
        stayMinMs = 300_000,
        stayMergeGapMs = 120_000,
    )

    private fun moveStayMovePoints(): List<TracePoint> {
        val stayLng = TestPoints.lngQOfMeters(20.0)
        return listOf(
            TestPoints.of(0, lngQ = TestPoints.lngQOfMeters(-100.0), speedMps = 2f),
            TestPoints.of(1_000, lngQ = TestPoints.lngQOfMeters(-90.0), speedMps = 2f),
            TestPoints.of(2_000, lngQ = stayLng),
            TestPoints.of(302_000, lngQ = stayLng),
            TestPoints.of(602_000, lngQ = stayLng),
            TestPoints.of(603_000, lngQ = TestPoints.lngQOfMeters(100.0), speedMps = 2f),
            TestPoints.of(604_000, lngQ = TestPoints.lngQOfMeters(110.0), speedMps = 2f),
        )
    }

    @Test
    fun `全 MOVE 产生一段且里程用原始点计算`() {
        val points =
            listOf(
                TestPoints.of(0, lngQ = 0),
                TestPoints.of(1_000, lngQ = TestPoints.lngQOfMeters(10.0)),
                TestPoints.of(2_000, lngQ = TestPoints.lngQOfMeters(20.0)),
            )

        val drafts = build(points, sessionId = "")

        assertEquals(1, drafts.size)
        val draft = drafts.single()
        assertEquals(SegmentKind.MOVE, draft.kind)
        assertEquals(Ids.segmentId("u-1", null, 0), draft.segmentId)
        assertEquals(GeoMath.pathLengthM(points), draft.distanceM, 1e-6)
        assertTrue(draft.geometryWkt.startsWith("LINESTRING("))
        assertNull(draft.stayLatQ)
        assertNull(draft.stayLngQ)
        assertEquals(3, draft.pointCount)
    }

    @Test
    fun `MOVE STAY MOVE 覆盖全部点且 STAY 带质心`() {
        val points = moveStayMovePoints()
        val drafts = build(points)

        assertEquals(listOf(SegmentKind.MOVE, SegmentKind.STAY, SegmentKind.MOVE), drafts.map { it.kind })
        assertEquals(points.size, drafts.sumOf { it.pointCount })

        val stay = drafts[1]
        assertEquals(0.0, stay.distanceM, 1e-9)
        assertNotNull(stay.stayLatQ)
        assertNotNull(stay.stayLngQ)
        assertTrue(stay.geometryWkt.startsWith("POINT("))

        assertEquals(2, drafts[0].pointCount)
        assertEquals(2, drafts[2].pointCount)
        assertNull(drafts[0].stayLatQ)
        assertNull(drafts[2].stayLatQ)
    }

    @Test
    fun `单点 MOVE 的几何退化为 POINT`() {
        val drafts = build(listOf(TestPoints.of(0)))

        val draft = drafts.single()
        assertEquals(SegmentKind.MOVE, draft.kind)
        assertEquals(1, draft.pointCount)
        assertEquals(0.0, draft.distanceM, 1e-9)
        assertTrue(draft.geometryWkt.startsWith("POINT("))
    }

    @Test
    fun `共线点被抽稀但里程不丢`() {
        val points = (0 until 20).map { i -> TestPoints.of(i.toLong() * 1_000, lngQ = TestPoints.lngQOfMeters(i.toDouble())) }
        val draft = build(points).single()

        assertEquals(1, draft.geometryWkt.count { it == ',' }, "WKT=${draft.geometryWkt}")
        assertEquals(GeoMath.pathLengthM(points), draft.distanceM, 1e-6)
    }

    @Test
    fun `同一输入重算两次结果完全相同`() {
        val points = moveStayMovePoints()
        assertEquals(build(points), build(points))
    }
}
