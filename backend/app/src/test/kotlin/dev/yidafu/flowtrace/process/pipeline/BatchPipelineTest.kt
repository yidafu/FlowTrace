package dev.yidafu.flowtrace.process.pipeline

import dev.yidafu.flowtrace.process.clean.DropReason
import dev.yidafu.flowtrace.shared.model.SegmentStatus
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * 批处理主链路测试（06 §3.2 / §3.6，08 §12）。
 *
 * 覆盖的是「分支选错就静默丢数据」的几处判断，而不是清洗/分段的算法细节
 * （算法在 [dev.yidafu.flowtrace.process.clean.PointCleaner] 与 segment 包各自的用例里）：
 * - 墓碑命中：整批丢弃，且**不能**顺手 markProcessed / 刷墓碑 TTL；
 * - 迟到批：点照常落库，但跳过分段与批状态更新（留给重算）；
 * - 重算锁占用：同样跳过分段；迟到优先于重算锁（`recalcSkipped` 只标记「非迟到」的情况）；
 * - 正常批：落点 → 分段（尾部窗口 OPEN）→ 置 PROCESSED。
 */
class BatchPipelineTest {
    @Test
    fun `正常批落点 分段 并置 PROCESSED`() {
        val fixture = PipelineFixture()
        val points =
            listOf(
                PipelineFixture.point(1_000),
                PipelineFixture.point(2_000, lngQ = PipelineFixture.LNG_Q + 10),
                PipelineFixture.point(3_000, lngQ = PipelineFixture.LNG_Q + 20),
            )

        val result = fixture.pipeline.process(fixture.message(points))

        assertEquals(3, result.pointsIn)
        assertEquals(3, result.kept)
        assertTrue(result.dropped.isEmpty(), "这批点不该有点被丢弃：${result.droppedText()}")
        assertFalse(result.late)
        assertFalse(result.tombstoned)

        assertEquals(3, fixture.writtenPoints.size)
        assertEquals(listOf(UUID.fromString(PipelineFixture.BATCH_ID)), fixture.processedBatchIds)

        val write = fixture.segmentWrites.single()
        assertEquals(1_000L, write.fromTsMs, "窗口重写必须从窗口首点开始")
        assertEquals(SegmentStatus.OPEN, write.status, "在线分段尾部窗口应以 OPEN 落库，等下一批续写")
        assertEquals(1, write.drafts.size, "三个同向点抽稀后应是一段 MOVE")
        assertEquals(result.segments, write.drafts.size, "返回的段数要与实际写入的草稿数一致")

        assertEquals(listOf(PipelineFixture.DEVICE_ID to 3_000L), fixture.watermarkWrites, "水位只推进到批内最大 ts")
    }

    @Test
    fun `墓碑命中整批丢弃并置 FAILED TOMBSTONE`() {
        val fixture = PipelineFixture()
        fixture.tombstoneHit = true

        val result = fixture.pipeline.process(fixture.message(listOf(PipelineFixture.point(1_000))))

        assertTrue(result.tombstoned)
        assertEquals(0, result.kept)
        assertEquals(1, result.pointsIn)
        assertTrue(fixture.writtenPoints.isEmpty(), "墓碑用户的点不能落库")
        assertTrue(fixture.segmentWrites.isEmpty())
        assertTrue(fixture.processedBatchIds.isEmpty(), "丢弃的批不能被标成 PROCESSED")
        assertEquals(
            listOf(UUID.fromString(PipelineFixture.BATCH_ID) to "TOMBSTONE"),
            fixture.failedBatches,
            "墓碑丢弃要落台账（06 §3.2 第 2 步），否则对账分不清「没处理」与「有意丢弃」",
        )
        assertTrue(fixture.watermarkWrites.isEmpty(), "整批丢弃时不该推进水位")
    }

    @Test
    fun `迟到批点照常落库并登记重算`() {
        val fixture = PipelineFixture()
        fixture.watermark = 10_000_000L
        val points =
            listOf(
                PipelineFixture.point(1_000_000),
                PipelineFixture.point(1_001_000),
            )

        val result = fixture.pipeline.process(fixture.message(points))

        assertTrue(result.late, "批内最小 ts 远早于水位应判迟到")
        assertEquals(2, result.kept)
        assertEquals(2, fixture.writtenPoints.size, "迟到的点仍要落库，否则重算没有输入")
        assertTrue(fixture.segmentWrites.isEmpty(), "迟到批跳过分段，避免用旧点覆盖已收尾的段")
        assertEquals(1, fixture.enqueuedDays.size, "迟到批必须登记 dirty_day，否则这段轨迹永远没有段")
        assertEquals(PipelineFixture.USER_ID, fixture.enqueuedDays.single().first)
        assertEquals(1, result.queuedDays)
        assertEquals(
            listOf(UUID.fromString(PipelineFixture.BATCH_ID)),
            fixture.processedBatchIds,
            "点已落库、补偿已登记，该批在本服务的职责结束",
        )
        assertFalse(result.recalcSkipped)
    }

    @Test
    fun `重算锁占用跳过分段但仍登记重算`() {
        val fixture = PipelineFixture()
        fixture.recalcLocked = true
        val points = listOf(PipelineFixture.point(1_000), PipelineFixture.point(2_000))

        val result = fixture.pipeline.process(fixture.message(points))

        assertTrue(result.recalcSkipped)
        assertFalse(result.late)
        assertEquals(2, fixture.writtenPoints.size)
        assertTrue(fixture.segmentWrites.isEmpty(), "重算期间分段由重算任务独占，在线链路必须让路")
        assertEquals(1, fixture.enqueuedDays.size, "让路的批同样要靠重算补段")
        assertEquals(1, fixture.processedBatchIds.size)
    }

    @Test
    fun `重算锁查询失败时按已加锁处理`() {
        val fixture = PipelineFixture()
        fixture.recalcLockFailure = RuntimeException("redis down")

        val result = fixture.pipeline.process(fixture.message(listOf(PipelineFixture.point(1_000))))

        assertTrue(result.recalcSkipped, "锁不可用要宁严不宽：跳过分段，避免与重算互删")
        assertEquals(1, fixture.enqueuedDays.size)
        assertTrue(fixture.segmentWrites.isEmpty())
    }

    @Test
    fun `水位不可用降级为非迟到且不推进水位`() {
        val fixture = PipelineFixture()
        fixture.watermarkFailure = RuntimeException("redis down")
        val points = listOf(PipelineFixture.point(1_000), PipelineFixture.point(2_000))

        val result = fixture.pipeline.process(fixture.message(points))

        assertFalse(result.late, "水位读不到时不能把正常批误判为迟到（那会让段永久缺失）")
        assertTrue(result.watermarkDegraded)
        assertTrue(fixture.watermarkWrites.isEmpty(), "降级时不推进水位")
        assertFalse(fixture.segmentWrites.isEmpty(), "降级后仍要正常分段")
        assertTrue(fixture.enqueuedDays.isEmpty())
    }

    @Test
    fun `迟到且加锁时只标迟到`() {
        val fixture = PipelineFixture()
        fixture.watermark = 10_000_000L
        fixture.recalcLocked = true

        val result = fixture.pipeline.process(fixture.message(listOf(PipelineFixture.point(1_000_000))))

        assertTrue(result.late)
        assertFalse(result.recalcSkipped, "late 优先，recalcSkipped 只表达『因加锁而跳过』")
    }

    @Test
    fun `清洗后没有点时不写库但仍收尾批状态`() {
        val fixture = PipelineFixture()
        val points =
            listOf(
                PipelineFixture.point(1_000, latQ = 0, lngQ = 0),
                PipelineFixture.point(2_000, latQ = 0, lngQ = 0),
            )

        val result = fixture.pipeline.process(fixture.message(points))

        assertEquals(0, result.kept)
        assertEquals(2, result.dropped[DropReason.ZERO])
        assertTrue(fixture.writtenPoints.isEmpty(), "全零岛不该产生任何 trace_point 行")
        assertTrue(fixture.segmentWrites.isEmpty(), "窗口里没有点，flush 不应写空段")
        assertEquals(1, fixture.processedBatchIds.size, "批已处理完（只是点全被过滤），仍要收尾")
    }

    @Test
    fun `丢弃原因压成一行便于单条日志`() {
        val result =
            BatchResult(
                pointsIn = 4,
                kept = 1,
                dropped = linkedMapOf(DropReason.ZERO to 3, DropReason.SPEED to 1),
            )

        assertEquals("zero=3,speed=1", result.droppedText())
        assertEquals("-", BatchResult(pointsIn = 1, kept = 1, dropped = emptyMap()).droppedText())
    }
}
