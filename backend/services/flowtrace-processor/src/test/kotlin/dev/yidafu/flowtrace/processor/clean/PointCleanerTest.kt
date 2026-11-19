package dev.yidafu.flowtrace.processor.clean

import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.model.GeoMath
import dev.yidafu.flowtrace.model.Ids
import dev.yidafu.flowtrace.model.LocationSource
import dev.yidafu.flowtrace.model.PointFlags
import dev.yidafu.flowtrace.model.TracePoint
import dev.yidafu.flowtrace.processor.config.ProcessorProperties
import dev.yidafu.flowtrace.processor.segment.DeviceState
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * 点级清洗测试（06 §3.3 规则表）。
 *
 * 三条跨批语义最容易写错，用例按「错一处的后果」组织：
 * - 所有时间/速度/精度规则都跟**上一个保留点**比（可能在上一批里），不是跟批内前一个点比；
 * - 被丢弃的点**不更新** `lastKept`，否则连续漂移能把基准一路拖走；
 * - 乱序点（dt < 0）只跳过时间类规则，不在这里丢，也不能让 `lastKept` 回退。
 */
class PointCleanerTest {
    private val props = ProcessorProperties()
    private val cleaner = PointCleaner(props)
    private val batchId = UUID.randomUUID().toString()

    @Test
    fun `零岛被丢弃并计入计数`() {
        val state = freshState()

        val result = cleaner.clean(listOf(point(TS), point(TS + 1_000, latQ = 0, lngQ = 0)), state, NOW, batchId)

        assertEquals(1, result.points.size)
        assertEquals(1, result.dropped[DropReason.ZERO])
    }

    @Test
    fun `FORCED 关键点跳过全部规则`() {
        val state = freshState()
        // 零岛 + 未来时间：正常情况下两条规则都会命中
        val forced =
            point(
                tsMs = NOW + props.futureMs + 1,
                latQ = 0,
                lngQ = 0,
                flags = PointFlags.FORCED,
            )

        val result = cleaner.clean(listOf(forced), state, NOW, batchId)

        assertEquals(1, result.points.size, "关键点必须优先保留")
        assertTrue(result.dropped.isEmpty())
    }

    @Test
    fun `超过未来容忍上限的点被丢弃`() {
        val state = freshState()

        val result = cleaner.clean(listOf(point(NOW + props.futureMs + 1)), state, NOW, batchId)

        assertEquals(0, result.points.size)
        assertEquals(1, result.dropped[DropReason.FUTURE])
    }

    @Test
    fun `时间戳与坐标完全相同的点算重复`() {
        val state = freshState()

        val result = cleaner.clean(listOf(point(TS), point(TS)), state, NOW, batchId)

        assertEquals(1, result.points.size)
        assertEquals(1, result.dropped[DropReason.DUPLICATE])
    }

    @Test
    fun `同一毫秒发生位移按物理不可能丢弃`() {
        val state = freshState()

        val result =
            cleaner.clean(
                listOf(point(TS), point(TS, latQ = LAT_Q + metersNorth(10.0))),
                state,
                NOW,
                batchId,
            )

        assertEquals(1, result.points.size)
        assertEquals(1, result.dropped[DropReason.SPEED])
    }

    @Test
    fun `隐含速度超阈值的点按漂移丢弃`() {
        val state = freshState()
        // 1 秒移动 1km ≈ 1000 m/s，远超 85 m/s
        val jump = point(TS + 1_000, latQ = LAT_Q + metersNorth(1_000.0))

        val result = cleaner.clean(listOf(point(TS), jump), state, NOW, batchId)

        assertEquals(1, result.points.size)
        assertEquals(1, result.dropped[DropReason.SPEED])
    }

    @Test
    fun `精度过差且间隔很短时丢弃`() {
        val state = freshState()
        val noisy = point(TS + 1_000, accuracyCm = props.cleanAccuracyCm + 1)

        val result = cleaner.clean(listOf(point(TS), noisy), state, NOW, batchId)

        assertEquals(1, result.points.size)
        assertEquals(1, result.dropped[DropReason.ACCURACY])
    }

    @Test
    fun `精度过差但间隔超过孤立窗口时保留`() {
        val state = freshState()
        val lonely = point(TS + props.cleanAccuracyLoneMs + 1, accuracyCm = props.cleanAccuracyCm * 3)

        val result = cleaner.clean(listOf(point(TS), lonely), state, NOW, batchId)

        assertEquals(2, result.points.size, "长途/长时间静止后的差精度点不能丢，否则整段轨迹断掉")
        assertTrue(result.dropped.isEmpty())
    }

    @Test
    fun `与上一批的保留点比较而不只是批内比较`() {
        val state = freshState()
        state.lastKept = tracePoint(TS)
        // 相对 state.lastKept 的 1 秒 1km 跳变
        val jump = point(TS + 1_000, latQ = LAT_Q + metersNorth(1_000.0))

        val result = cleaner.clean(listOf(jump), state, NOW, batchId)

        assertEquals(0, result.points.size, "跨批基准必须生效，否则每批首个漂移点都会漏过速度规则")
        assertEquals(1, result.dropped[DropReason.SPEED])
    }

    @Test
    fun `乱序点保留且不让 lastKept 回退`() {
        val state = freshState()
        state.lastKept = tracePoint(TS + 5_000, latQ = LAT_Q + metersNorth(500.0))
        val outOfOrder = point(TS, latQ = LAT_Q)

        val result = cleaner.clean(listOf(outOfOrder), state, NOW, batchId)

        assertEquals(1, result.points.size, "乱序点的时间类规则无从判断，不该在清洗阶段丢")
        assertEquals(TS + 5_000, state.lastKept?.tsMs, "lastKept 不能因为乱序点回退")
    }

    @Test
    fun `被丢弃的点不更新 lastKept`() {
        val state = freshState()
        val drift = point(TS + 1_000, latQ = LAT_Q + metersNorth(1_000.0))
        val back = point(TS + 2_000, latQ = LAT_Q + metersNorth(1.0))

        val result = cleaner.clean(listOf(point(TS), drift, back), state, NOW, batchId)

        assertEquals(2, result.points.size)
        assertEquals(1, result.dropped[DropReason.SPEED])
        assertEquals(TS + 2_000, state.lastKept?.tsMs)
    }

    @Test
    fun `协议缺省的 0 归一为 null 而真实值原样保留`() {
        val state = freshState()
        val filled =
            point(
                TS,
                accuracyCm = 500,
                speedMps = 1.5f,
                bearingDeg = 90,
                altitudeCm = 1_200,
                source = 1, // SOURCE_GPS
            )

        val cleaned = cleaner.clean(listOf(point(TS + 1_000), filled), state, NOW, batchId).points

        val empty = cleaned.first()
        assertNull(empty.accuracyCm)
        assertNull(empty.speedMps)
        assertNull(empty.bearingDeg)
        assertNull(empty.altitudeCm)
        // 协议约定「缺省按 FUSED 处理」是端侧语义，落库的 0 就是 UNSPECIFIED，不做替换
        assertEquals(LocationSource.UNSPECIFIED, empty.source)

        val full = cleaned.last()
        assertEquals(500, full.accuracyCm)
        assertEquals(1.5f, full.speedMps)
        assertEquals(90.toShort(), full.bearingDeg)
        assertEquals(1_200, full.altitudeCm)
        assertEquals(LocationSource.GPS, full.source)
    }

    @Test
    fun `点上身份信息取自设备缓存与 batchId`() {
        val state = freshState()
        val raw = point(TS, idx = 7)

        val cleaned = cleaner.clean(listOf(raw), state, NOW, batchId).points.single()

        assertEquals(USER_ID, cleaned.userId)
        assertEquals(DEVICE_ID, cleaned.deviceId)
        assertEquals(SESSION_ID, cleaned.sessionId)
        assertEquals(TASK_ID, cleaned.taskId)
        assertEquals(UUID.fromString(batchId), cleaned.batchId)
        // pointId 按「批内序号」派生，不是按循环下标；重放同批必须得到同一个 id
        assertEquals(Ids.pointId(batchId, 7), cleaned.pointId)
    }

    // ------------------------------------------------------------------ 脚手架

    private fun freshState() =
        DeviceState(
            deviceId = DEVICE_ID,
            userId = USER_ID,
            sessionId = SESSION_ID,
            taskId = TASK_ID,
        )

    private fun tracePoint(
        tsMs: Long,
        latQ: Int = LAT_Q,
        lngQ: Int = LNG_Q,
    ) = TracePoint(
        tsMs = tsMs,
        pointId = UUID.randomUUID(),
        userId = USER_ID,
        deviceId = DEVICE_ID,
        batchId = UUID.fromString(batchId),
        latQ = latQ,
        lngQ = lngQ,
    )

    private fun point(
        tsMs: Long,
        latQ: Int = LAT_Q,
        lngQ: Int = LNG_Q,
        accuracyCm: Int = 0,
        speedMps: Float = 0f,
        bearingDeg: Int = 0,
        altitudeCm: Int = 0,
        source: Int = 0,
        flags: Int = 0,
        idx: Int = 0,
    ): TraceMessage.AbsolutePoint =
        TraceMessage.AbsolutePoint
            .newBuilder()
            .setTsMs(tsMs)
            .setLatQ(latQ)
            .setLngQ(lngQ)
            .setAccuracyCm(accuracyCm)
            .setSpeedMps(speedMps)
            .setBearingDeg(bearingDeg)
            .setAltitudeCm(altitudeCm)
            .setSource(source)
            .setFlags(flags)
            .setIdx(idx)
            .build()

    /** 纬度方向 1° ≈ 111195m，测试只关心量级。 */
    private fun metersNorth(meters: Double): Int = GeoMath.quantize(meters / 111_195.0)

    private companion object {
        const val USER_ID = "u-1"
        const val DEVICE_ID = "dev-1"
        const val SESSION_ID = "s-1"
        const val TASK_ID = "t-1"
        const val LAT_Q = 390000000
        const val LNG_Q = 1160000000

        /** 固定「当前时刻」，所有测试点都在它的过去，避免依赖机器时钟。 */
        const val NOW = 1_700_000_000_000L
        const val TS = NOW - 100_000L
    }
}
