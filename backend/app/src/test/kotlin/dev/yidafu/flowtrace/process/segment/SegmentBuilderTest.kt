package dev.yidafu.flowtrace.process.segment

import dev.yidafu.flowtrace.shared.model.GeoMath
import dev.yidafu.flowtrace.shared.model.Ids
import dev.yidafu.flowtrace.shared.model.SegmentKind
import dev.yidafu.flowtrace.shared.model.TracePoint
import dev.yidafu.flowtrace.shared.model.TraceSegmentDraft
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNotSame
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class SegmentBuilderTest {
    private val builder = SegmentBuilder()

    private fun build(
        points: List<TracePoint>,
        sessionId: String? = null,
        reusable: Map<Long, TraceSegmentDraft> = emptyMap(),
    ) = builder.build(
        userId = "u-1",
        sessionId = sessionId,
        points = points,
        epsilonM = 5.0,
        stayRadiusM = 50.0,
        stayMinMs = 300_000,
        stayMergeGapMs = 120_000,
        reusable = reusable,
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

    @Test
    fun `未变化的区间原样复用 只有尾巴重算`() {
        val points = moveStayMovePoints()
        val first = build(points)

        // 尾部再追加两个 MOVE 点：前两段区间（MOVE 0..1 / STAY 2..4）没变，末段 MOVE 5..6 → 5..8
        val extended =
            points +
                TestPoints.of(605_000, lngQ = TestPoints.lngQOfMeters(120.0), speedMps = 2f) +
                TestPoints.of(606_000, lngQ = TestPoints.lngQOfMeters(130.0), speedMps = 2f)
        val second = build(extended, reusable = first.associateBy { it.startTsMs })

        assertSame(first[0], second[0], "区间没变的段不该重算（DP/里程/WKT 全是 O(区间点数)）")
        assertSame(first[1], second[1])
        assertNotSame(first[2], second[2], "末段区间被新点拉长了，必须重算")
        // 复用不能改变结果：与完全不复用的构建逐字段相同
        assertEquals(build(extended), second)
    }
}
