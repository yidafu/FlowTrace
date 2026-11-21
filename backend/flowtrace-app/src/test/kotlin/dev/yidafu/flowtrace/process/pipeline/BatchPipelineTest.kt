package dev.yidafu.flowtrace.process.pipeline

import dev.yidafu.flowtrace.process.clean.DropReason
import dev.yidafu.flowtrace.shared.model.SegmentStatus
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * 批处理主链路测试（06 §3.2 / §3.6，08 §4、§12）。
 *
 * 覆盖的是「分支选错就静默丢数据」的几处判断，而不是清洗/分段的算法细节
 * （算法在 [dev.yidafu.flowtrace.process.clean.PointCleaner] 与 segment 包各自的用例里）：
 * - 墓碑命中：整批丢弃，且**不能**顺手 markProcessed / 刷墓碑 TTL；
 * - 迟到批：点照常落库，但跳过分段与批状态更新（留给重算）；
 * - 重算锁占用：同样跳过分段；迟到优先于重算锁（`recalcSkipped` 只标记「非迟到」的情况）；
 * - 正常批：落点 → 分段（尾部窗口 OPEN）→ 置 PROCESSED；
 * - 合批（08 §4）：一个 cycle 共用一次点 INSERT / 一次水位推进，分支批（墓碑/迟到）
 *   只影响自己、不拖累同 cycle 的正常批。
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
    fun `合批两批共用一个事务 一次写点 逐批分段`() {
        val fixture = PipelineFixture()
        val pointsA =
            listOf(
                PipelineFixture.point(1_000),
                PipelineFixture.point(2_000, lngQ = PipelineFixture.LNG_Q + 10),
                PipelineFixture.point(3_000, lngQ = PipelineFixture.LNG_Q + 20),
            )
        val pointsB =
            listOf(
                PipelineFixture.point(1_100),
                PipelineFixture.point(2_100, lngQ = PipelineFixture.LNG_Q + 10),
                PipelineFixture.point(3_100, lngQ = PipelineFixture.LNG_Q + 20),
            )
        val first =
            fixture.message(
                pointsA,
                batchId = "0190f2a0-0000-7000-8000-0000000000b1",
                deviceId = "dev-1",
                userId = "u-1",
            )
        val second =
            fixture.message(
                pointsB,
                batchId = "0190f2a0-0000-7000-8000-0000000000b2",
                deviceId = "dev-2",
                userId = "u-2",
            )

        val results = fixture.pipeline.processAll(listOf(first, second))

        assertEquals(2, results.size)
        results.forEach {
            assertEquals(3, it.kept)
            assertFalse(it.tombstoned)
            assertFalse(it.late)
        }
        assertEquals(1, fixture.pointsWriteCalls, "一个 cycle 的点必须合进一次 INSERT")
        assertEquals(6, fixture.writtenPoints.size)
        assertEquals(2, fixture.segmentWrites.size, "分段仍逐批 flush（每设备一次）")
        fixture.segmentWrites.forEach { assertEquals(SegmentStatus.OPEN, it.status) }
        assertEquals(2, fixture.processedBatchIds.size)
        assertEquals(
            listOf("dev-1" to 3_000L, "dev-2" to 3_100L),
            fixture.watermarkWrites,
            "一次多键 EVAL 推进两个设备的水位",
        )
    }

    @Test
    fun `合批同设备多批水位只推进一次取最大`() {
        val fixture = PipelineFixture()
        val first =
            fixture.message(
                listOf(
                    PipelineFixture.point(1_000),
                    PipelineFixture.point(3_000, lngQ = PipelineFixture.LNG_Q + 10),
                ),
                batchId = "0190f2a0-0000-7000-8000-0000000000c1",
            )
        val second =
            fixture.message(
                listOf(
                    PipelineFixture.point(4_000),
                    PipelineFixture.point(6_000, lngQ = PipelineFixture.LNG_Q + 10),
                ),
                batchId = "0190f2a0-0000-7000-8000-0000000000c2",
            )

        fixture.pipeline.processAll(listOf(first, second))

        assertEquals(1, fixture.pointsWriteCalls)
        assertEquals(4, fixture.writtenPoints.size)
        assertEquals(2, fixture.segmentWrites.size, "同设备两批仍各 flush 一次（按 poll 顺序保序）")
        assertEquals(
            listOf(PipelineFixture.DEVICE_ID to 6_000L),
            fixture.watermarkWrites,
            "同设备多批只传一次、取最大批尾 ts：writeIfGreater 单调，与逐批写终态一致",
        )
        assertEquals(2, fixture.processedBatchIds.size)
    }

    @Test
    fun `合批中墓碑批整批丢弃 其余批照常`() {
        val fixture = PipelineFixture()
        fixture.tombstoneUsers = setOf(PipelineFixture.USER_ID)
        val doomed =
            fixture.message(
                listOf(PipelineFixture.point(1_000), PipelineFixture.point(2_000, lngQ = PipelineFixture.LNG_Q + 10)),
                batchId = "0190f2a0-0000-7000-8000-0000000000d1",
                deviceId = "dev-1",
                userId = "u-1",
            )
        val normal =
            fixture.message(
                listOf(PipelineFixture.point(1_500), PipelineFixture.point(2_500, lngQ = PipelineFixture.LNG_Q + 10)),
                batchId = "0190f2a0-0000-7000-8000-0000000000d2",
                deviceId = "dev-2",
                userId = "u-2",
            )

        val results = fixture.pipeline.processAll(listOf(doomed, normal))

        assertTrue(results[0].tombstoned)
        assertEquals(0, results[0].kept)
        assertFalse(results[1].tombstoned)
        assertEquals(2, results[1].kept)
        assertEquals(1, fixture.pointsWriteCalls, "墓碑批剔除后，剩余批仍合一次 INSERT")
        assertEquals(2, fixture.writtenPoints.size, "墓碑批的点不能落库，正常批的点照常")
        assertEquals(
            listOf(UUID.fromString("0190f2a0-0000-7000-8000-0000000000d1") to "TOMBSTONE"),
            fixture.failedBatches,
        )
        assertEquals(listOf(UUID.fromString("0190f2a0-0000-7000-8000-0000000000d2")), fixture.processedBatchIds)
        assertEquals(1, fixture.segmentWrites.size, "只有正常批走分段")
        assertEquals(
            listOf("dev-2" to 2_500L),
            fixture.watermarkWrites,
            "墓碑批不参与水位推进",
        )
    }

    @Test
    fun `合批中迟到批跳过分段 正常批照常`() {
        val fixture = PipelineFixture()
        fixture.watermark = 10_000_000L
        val late =
            fixture.message(
                listOf(PipelineFixture.point(1_000_000), PipelineFixture.point(1_001_000)),
                batchId = "0190f2a0-0000-7000-8000-0000000000e1",
                deviceId = "dev-1",
            )
        val normal =
            fixture.message(
                listOf(
                    PipelineFixture.point(9_500_000),
                    PipelineFixture.point(9_501_000, lngQ = PipelineFixture.LNG_Q + 10),
                ),
                batchId = "0190f2a0-0000-7000-8000-0000000000e2",
                deviceId = "dev-2",
            )

        val results = fixture.pipeline.processAll(listOf(late, normal))

        assertTrue(results[0].late)
        assertTrue(results[0].queuedDays > 0, "迟到批要登记 dirty_day")
        assertFalse(results[1].late)
        assertEquals(1, fixture.pointsWriteCalls)
        assertEquals(4, fixture.writtenPoints.size, "迟到批与正常批的点合并进同一次 INSERT（2+2），迟到的点不落库重算就没有输入")
        assertEquals(1, fixture.enqueuedDays.size)
        assertEquals(PipelineFixture.USER_ID, fixture.enqueuedDays.single().first)
        assertEquals(1, fixture.segmentWrites.size, "迟到批跳过分段，正常批照常 flush")
        assertEquals(2, fixture.processedBatchIds.size, "迟到与正常批都置 PROCESSED（职责都已结束）")
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
