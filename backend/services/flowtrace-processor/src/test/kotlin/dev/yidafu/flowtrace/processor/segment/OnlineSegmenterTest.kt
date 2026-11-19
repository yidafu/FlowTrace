package dev.yidafu.flowtrace.processor.segment

import dev.yidafu.flowtrace.model.GeoMath
import dev.yidafu.flowtrace.model.MoveMode
import dev.yidafu.flowtrace.model.SegmentStatus
import dev.yidafu.flowtrace.model.TracePoint
import dev.yidafu.flowtrace.model.TraceSegmentDraft
import dev.yidafu.flowtrace.processor.config.ProcessorProperties
import dev.yidafu.flowtrace.processor.pipeline.BatchMeta
import dev.yidafu.flowtrace.storage.postgres.repository.DeviceStateLoader
import dev.yidafu.flowtrace.storage.postgres.repository.SegmentScope
import dev.yidafu.flowtrace.storage.postgres.repository.TraceSegmentRepository
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * 在线分段测试（06 §3.4，08 §8.2）。
 *
 * 重点是三类「切分/落库状态」判断：
 * - 切分条件互不替代：时间断层 / 位移跳变 / 模式变化，任一命中都要闭合旧窗口；
 * - 状态语义：切分与满点强制闭合写 FINALIZED，批末 flush 写 OPEN（等下一批续写）；
 *   只有水位已落后时才把批末窗口也判 FINALIZED；
 * - 乱序点跳过分段但不该影响窗口收尾。
 */
class OnlineSegmenterTest {
    private val props = ProcessorProperties()
    private val writes = mutableListOf<Write>()
    private val finalizedBefore = mutableListOf<Long>()

    private val repository =
        object : TraceSegmentRepository() {
            override fun replaceTail(
                scope: SegmentScope,
                fromTsMs: Long,
                drafts: List<TraceSegmentDraft>,
                status: SegmentStatus,
            ) {
                writes += Write(fromTsMs, drafts, status)
            }

            override fun finalizeOpenBefore(
                scope: SegmentScope,
                beforeTsMs: Long,
            ): Int {
                finalizedBefore += beforeTsMs
                return 0
            }
        }

    private val store = DeviceStateStore(props, emptyLoader())
    private val segmenter = OnlineSegmenter(store, props, repository, SegmentBuilder(), MoveModeClassifier())

    @Test
    fun `连续点只产生一个 OPEN 窗口`() {
        val points = listOf(point(0), point(1_000, latQ = lat(5.0)), point(2_000, latQ = lat(10.0)))

        val written = segmenter.onBatch(meta(points), points, watermark = null)

        val write = writes.single()
        assertEquals(0L, write.fromTsMs)
        assertEquals(SegmentStatus.OPEN, write.status)
        assertEquals(1, write.drafts.size)
        assertEquals(written, write.drafts.size)
    }

    @Test
    fun `时间断层切分 旧窗口闭合为 FINALIZED`() {
        val points = listOf(point(0), point(1_000), point(1_000 + props.splitGapMs + 1))

        segmenter.onBatch(meta(points), points, watermark = null)

        assertEquals(2, writes.size)
        assertEquals(SegmentStatus.FINALIZED, writes[0].status, "切分点前的窗口必须封口")
        assertEquals(0L, writes[0].fromTsMs)
        assertEquals(1_000 + props.splitGapMs + 1, writes[1].fromTsMs, "新窗口从切分点起")
        assertEquals(SegmentStatus.OPEN, writes[1].status, "批末窗口仍可续写")
        assertEquals(
            1,
            store
                .getOrLoad(DEVICE_ID)
                .open
                ?.points
                ?.size,
            "新窗口只包含切分点本身",
        )
    }

    @Test
    fun `位移跳变切分`() {
        val points = listOf(point(0), point(1_000, latQ = lat(props.splitJumpM + 1_000)))

        segmenter.onBatch(meta(points), points, watermark = null)

        assertEquals(2, writes.size)
        assertEquals(SegmentStatus.FINALIZED, writes[0].status)
        assertEquals(1, writes[1].drafts.size)
    }

    @Test
    fun `运动模式变化切分`() {
        val points = listOf(point(0, speedMps = 1f), point(1_000, speedMps = 10f))

        segmenter.onBatch(meta(points), points, watermark = null)

        assertEquals(2, writes.size, "速度从步行跳到骑行应切段")
        assertEquals(SegmentStatus.FINALIZED, writes[0].status)
        assertEquals(MoveMode.BICYCLE, store.getOrLoad(DEVICE_ID).open?.moveMode, "新窗口的模式取切分点的确认模式")
    }

    @Test
    fun `乱序点跳过分段且不影响窗口收尾`() {
        val points = listOf(point(0), point(1_000), point(500))

        segmenter.onBatch(meta(points), points, watermark = null)

        val write = writes.single()
        assertEquals(SegmentStatus.OPEN, write.status)
        assertEquals(2, write.drafts.single().pointCount, "乱序点不能进窗口，否则 WKT 与里程都会被污染")
    }

    @Test
    fun `点数达到上限强制闭合`() {
        props.openSegMaxPoints = 3
        val points = (0 until 5).map { point(it * 1_000L, latQ = lat(it * 5.0)) }

        val written = segmenter.onBatch(meta(points), points, watermark = null)

        assertEquals(2, writes.size)
        assertEquals(SegmentStatus.FINALIZED, writes[0].status)
        assertEquals(3, writes[0].drafts.sumOf { it.pointCount }, "到上限立即封口，不能把内存窗口拖大")
        assertEquals(2, writes[1].drafts.sumOf { it.pointCount })
        assertEquals(2, written)
    }

    @Test
    fun `水位已落后的窗口以 FINALIZED 收尾`() {
        val lastTs = 10_000L
        val points = listOf(point(0), point(lastTs))

        segmenter.onBatch(meta(points), points, watermark = lastTs + props.watermarkDriftMs + 1)

        assertEquals(SegmentStatus.FINALIZED, writes.single().status, "窗口末点已落在水位之外，后续不会再有同窗点")
        assertNotNull(store.getOrLoad(DEVICE_ID).open, "close=false 时内存窗口保留，等下一批续写")
    }

    @Test
    fun `没有 userId 时不写段`() {
        val state = DeviceState(DEVICE_ID)
        state.open = OpenWindow(0, mutableListOf(point(0)))

        assertEquals(0, segmenter.flush(state))
        assertTrue(writes.isEmpty(), "userId 是段身份的一部分，缺失时不能落库")
    }

    @Test
    fun `截断窗口恢复时先封口旧 OPEN 段再重写`() {
        val state = DeviceState(DEVICE_ID, userId = USER_ID, sessionId = SESSION_ID)
        state.openTruncated = true
        state.open = OpenWindow(5_000, mutableListOf(point(5_000), point(6_000)))

        segmenter.flush(state, close = false)

        assertEquals(listOf(5_000L), finalizedBefore, "必须先按旧窗口起点封口，否则旧 OPEN 段永远留在库里")
        assertFalse(state.openTruncated, "封口后要复位标记，避免每次 flush 重复扫库")
        assertEquals(SegmentStatus.OPEN, writes.single().status)
    }

    // ------------------------------------------------------------------ 脚手架

    private data class Write(
        val fromTsMs: Long,
        val drafts: List<TraceSegmentDraft>,
        val status: SegmentStatus,
    )

    private fun meta(points: List<TracePoint>) =
        BatchMeta(
            userId = USER_ID,
            deviceId = DEVICE_ID,
            sessionId = SESSION_ID,
            taskId = "",
            channel = 1,
            receivedTsMs = 0,
            minTsMs = points.first().tsMs,
            maxTsMs = points.last().tsMs,
        )

    private fun point(
        tsMs: Long,
        latQ: Int = LAT_Q,
        speedMps: Float? = null,
    ) = TracePoint(
        tsMs = tsMs,
        pointId = UUID.randomUUID(),
        userId = USER_ID,
        deviceId = DEVICE_ID,
        batchId = BATCH_ID,
        latQ = latQ,
        lngQ = LNG_Q,
        speedMps = speedMps,
    )

    /** 纬度方向偏移：1° ≈ 111195m，距离与入参同量级，便于直接与分段阈值比较。 */
    private fun lat(meters: Double): Int = LAT_Q + GeoMath.quantize(meters / 111_195.0)

    private fun emptyLoader() =
        object : DeviceStateLoader() {
            override fun loadLastKept(deviceId: String): TracePoint? = null

            override fun loadOpenStartTs(
                userId: String,
                deviceId: String,
            ): Instant? = null

            override fun loadWindow(
                userId: String,
                startTs: Instant,
                maxPoints: Int,
            ): List<TracePoint> = emptyList()
        }

    private companion object {
        const val USER_ID = "u-1"
        const val DEVICE_ID = "dev-1"
        const val SESSION_ID = "s-1"
        const val LAT_Q = 390000000
        const val LNG_Q = 1160000000
        val BATCH_ID: UUID = UUID.fromString("0190f2a0-0000-7000-8000-0000000000bb")
    }
}
