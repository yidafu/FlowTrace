package dev.yidafu.flowtrace.shared.model

import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.WKTReader
import java.util.UUID
import kotlin.math.PI
import kotlin.math.sin
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class GeoMathTest {
    private val originLatQ = 312_304_000 // 31.2304°
    private val originLngQ = 1_214_737_000 // 121.4737°
    private val batchId = UUID.fromString("018f2b3c-0000-7000-8000-000000000001")

    private fun pt(
        tsMs: Long,
        latQ: Int,
        lngQ: Int,
    ) = TracePoint(
        tsMs = tsMs,
        pointId = UUID(0, tsMs),
        userId = "u-1001",
        deviceId = "d-1",
        batchId = batchId,
        latQ = latQ,
        lngQ = lngQ,
    )

    // ---------------------------------------------------------------- 量化

    @Test
    fun `量化遵循 Math_round 语义且可逆`() {
        assertEquals(1_164_074_000, GeoMath.quantize(116.4074))
        assertEquals(399_042_000, GeoMath.quantize(39.9042))
        assertEquals(0, GeoMath.quantize(0.0))
        assertEquals(116.4074, GeoMath.dequantize(1_164_074_000), 1e-9)
        // 极值不溢出 int32：±90° → ±9e8，±180° → ±1.8e9
        assertEquals(900_000_000, GeoMath.quantize(90.0))
        assertEquals(1_800_000_000, GeoMath.quantize(180.0))
    }

    // ---------------------------------------------------------------- 距离

    @Test
    fun `haversine 与已知城市距离一致`() {
        // 北京 39.9042,116.4074 → 上海 31.2304,121.4737，大圆距离约 1067km
        val d =
            GeoMath.haversineM(
                GeoMath.quantize(39.9042),
                GeoMath.quantize(116.4074),
                GeoMath.quantize(31.2304),
                GeoMath.quantize(121.4737),
            )
        assertTrue(d in 1_060_000.0..1_075_000.0, "实际距离 $d m")
    }

    @Test
    fun `haversine 对称且同点为 0`() {
        val a = pt(0, originLatQ, originLngQ)
        val b = pt(1, originLatQ + 10_000, originLngQ + 10_000)
        assertEquals(0.0, GeoMath.haversineM(a.latQ, a.lngQ, a.latQ, a.lngQ), 1e-9)
        assertEquals(GeoMath.distanceM(a, b), GeoMath.distanceM(b, a), 1e-9)
    }

    @Test
    fun `initialBearing 正北为 0 正东为 90`() {
        val north = GeoMath.initialBearingDeg(0, 0, 100_000, 0)
        val east = GeoMath.initialBearingDeg(0, 0, 0, 100_000)
        assertEquals(0.0, north, 1e-6)
        assertEquals(90.0, east, 1e-6)
    }

    @Test
    fun `pathLengthM 为相邻点累加`() {
        val points = listOf(pt(0, originLatQ, originLngQ), pt(1, originLatQ + 10_000, originLngQ))
        // 0.001° 纬度 ≈ 111.19m
        assertEquals(111.19, GeoMath.pathLengthM(points), 0.5)
        assertEquals(0.0, GeoMath.pathLengthM(emptyList()), 0.0)
        assertEquals(0.0, GeoMath.pathLengthM(points.take(1)), 0.0)
    }

    // ---------------------------------------------------------------- 局部投影

    @Test
    fun `局部投影在米制下与 haversine 一致`() {
        val a = pt(0, originLatQ, originLngQ)
        val b = pt(1, originLatQ + 10_000, originLngQ + 10_000)
        val local = GeoMath.toLocalMeters(a.latQ, a.lngQ, listOf(a, b))
        val planar = kotlin.math.sqrt(local[2] * local[2] + local[3] * local[3])
        // 城市尺度下等距圆柱与球面距离相对误差应远小于 0.1%
        assertEquals(GeoMath.distanceM(a, b), planar, GeoMath.distanceM(a, b) * 1e-3)
    }

    @Test
    fun `局部投影反投影可还原量化坐标`() {
        val points =
            listOf(
                pt(0, originLatQ, originLngQ),
                pt(1, originLatQ + 12_345, originLngQ - 6_789),
                pt(2, originLatQ - 4_321, originLngQ + 9_876),
            )
        val local = GeoMath.toLocalMeters(originLatQ, originLngQ, points)
        val back = GeoMath.localToQuantized(originLatQ, originLngQ, local)
        // 允许 ±1 个量化单位（≈1cm）的双精度舍入误差，但不应更大
        points.forEachIndexed { i, p ->
            assertTrue(kotlin.math.abs(p.latQ - back[i * 2]) <= 1, "第 $i 点纬度还原误差过大：${p.latQ} vs ${back[i * 2]}")
            assertTrue(kotlin.math.abs(p.lngQ - back[i * 2 + 1]) <= 1, "第 $i 点经度还原误差过大：${p.lngQ} vs ${back[i * 2 + 1]}")
        }
    }

    // ---------------------------------------------------------------- 抽稀

    @Test
    fun `共线点被抽稀到只剩首尾`() {
        val points = (0 until 200).map { i -> pt(i.toLong(), originLatQ, originLngQ + i * 1_000) }
        val simplified = GeoMath.dpSimplifyMeters(points, 5.0)
        assertEquals(2, simplified.size)
        assertSame(points.first(), simplified.first())
        assertSame(points.last(), simplified.last())
    }

    @Test
    fun `偏离超过 epsilon 的顶点必须保留`() {
        val points =
            (0 until 21).map { i ->
                val detour = if (i == 10) 1_800 else 0 // 约 20m 侧偏
                pt(i.toLong(), originLatQ + detour, originLngQ + i * 1_000)
            }
        val simplified = GeoMath.dpSimplifyMeters(points, 5.0)
        // 拐点不能丢；但拐点两侧的长弦会切掉角，邻近点因偏距已超 ε 也会被保留（不可能只剩 3 点）
        assertContains(simplified, points[10])
        assertTrue(simplified.size < points.size, "偏离点未被抽稀")
        assertTrue(maxDeviationM(points, simplified) <= 5.0 + 1e-6)
    }

    @Test
    fun `抽稀结果满足 epsilon 上界且是原始点子序列`() {
        val points =
            (0 until 300).map { i ->
                val offsetQ = (1_800 * sin(i * PI / 60)).toInt() // 约 ±20m 的起伏
                pt(i.toLong(), originLatQ + offsetQ, originLngQ + i * 1_000)
            }
        val simplified = GeoMath.dpSimplifyMeters(points, 5.0)
        assertTrue(simplified.size < points.size / 2, "抽稀后点数 ${simplified.size} 未明显下降")
        // 子序列：每个保留点都是原对象（不是重新构造的近似点），且顺序递增
        assertTrue(simplified.all { s -> points.any { it === s } })
        assertTrue(maxDeviationM(points, simplified) <= 5.0 + 1e-6)
    }

    @Test
    fun `抽稀幂等且短序列原样返回`() {
        val points =
            (0 until 100).map { i ->
                val offsetQ = (1_800 * sin(i * PI / 30)).toInt()
                pt(i.toLong(), originLatQ + offsetQ, originLngQ + i * 1_000)
            }
        val once = GeoMath.dpSimplifyMeters(points, 5.0)
        val twice = GeoMath.dpSimplifyMeters(once, 5.0)
        assertEquals(once, twice)

        val short = points.take(2)
        assertEquals(short, GeoMath.dpSimplifyMeters(short, 5.0))
        assertEquals(points, GeoMath.dpSimplifyMeters(points, 0.0))
    }

    // ---------------------------------------------------------------- 质心 / WKT

    @Test
    fun `质心为量化坐标算术平均`() {
        val points = listOf(pt(0, 100, 200), pt(1, 200, 400), pt(2, 300, 600))
        val c = GeoMath.centroidQ(points)
        assertEquals(200, c[0])
        assertEquals(400, c[1])
    }

    @Test
    fun `LINESTRING WKT 可被 JTS 解析且经度在前`() {
        val points = listOf(pt(0, 312_304_000, 1_214_737_000), pt(1, 312_304_100, 1_214_737_100))
        val wkt = GeoMath.lineStringWkt(points)
        val geom = WKTReader().read(wkt)
        assertEquals(2, geom.numPoints)
        // WKT 是 (lng lat) 顺序：第一坐标 x=经度、y=纬度
        assertEquals(121.4737, geom.coordinates[0].x, 1e-9)
        assertEquals(31.2304, geom.coordinates[0].y, 1e-9)
    }

    @Test
    fun `单点线段退化为 POINT 而非非法 LINESTRING`() {
        val wkt = GeoMath.lineStringWkt(listOf(pt(0, 312_304_000, 1_214_737_000)))
        assertTrue(wkt.startsWith("POINT("))
        assertEquals(1, WKTReader().read(wkt).numPoints)
        assertEquals("POINT(121.4737 31.2304)", GeoMath.pointWkt(312_304_000, 1_214_737_000))
    }

    // ---------------------------------------------------------------- helpers

    /** 原始点到简化折线的最大偏距（米）：在局部米制平面内用 JTS 点到线距离。 */
    private fun maxDeviationM(
        original: List<TracePoint>,
        simplified: List<TracePoint>,
    ): Double {
        val localOriginal = GeoMath.toLocalMeters(original.first().latQ, original.first().lngQ, original)
        val localSimplified = GeoMath.toLocalMeters(original.first().latQ, original.first().lngQ, simplified)
        val factory = GeometryFactory()
        val line =
            factory.createLineString(
                Array(simplified.size) { Coordinate(localSimplified[it * 2], localSimplified[it * 2 + 1]) },
            )
        var max = 0.0
        for (i in original.indices) {
            val p = factory.createPoint(Coordinate(localOriginal[i * 2], localOriginal[i * 2 + 1]))
            max = maxOf(max, line.distance(p))
        }
        return max
    }

    private fun assertSame(
        expected: TracePoint,
        actual: TracePoint,
    ) {
        assertTrue(expected === actual, "抽稀应返回原始点对象而非新构造的点")
    }
}
