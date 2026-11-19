package dev.yidafu.flowtrace.model

import java.time.Instant
import java.time.LocalDate
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TracePointTest {
    private fun point(
        tsMs: Long,
        flags: Int = 0,
        latQ: Int = 399_042_000,
        lngQ: Int = 1_164_074_000,
    ) = TracePoint(
        tsMs = tsMs,
        pointId = UUID(0, tsMs),
        userId = "u-1001",
        deviceId = "d-1",
        batchId = UUID(0, 1),
        latQ = latQ,
        lngQ = lngQ,
        flags = flags,
    )

    @Test
    fun `utcDay 按 UTC 切日 - 不用本地时区`() {
        val utcMidnight = Instant.parse("2026-11-19T00:00:00Z").toEpochMilli()
        assertEquals(LocalDate.of(2026, 11, 19), point(utcMidnight).utcDay())
        // UTC 前 1 秒仍属前一日
        assertEquals(LocalDate.of(2026, 11, 18), point(utcMidnight - 1_000).utcDay())
        // 北京 2026-11-19 00:30 = UTC 2026-11-18 16:30 → 必须归到 11-18，否则重算范围与分区错位
        val beijing = Instant.parse("2026-11-19T00:30:00+08:00").toEpochMilli()
        assertEquals(LocalDate.of(2026, 11, 18), point(beijing).utcDay())
    }

    @Test
    fun `标志位按位判定而非等值`() {
        val combined = point(0, flags = PointFlags.ENTER_RADIUS or PointFlags.MOTION_CHANGE)
        assertTrue(combined.hasMotionChange)
        assertTrue(combined.hasFlag(PointFlags.ENTER_RADIUS))
        assertFalse(combined.hasFlag(PointFlags.LOW_BATTERY))
        // 未知位不影响已知位判定（前向兼容）
        val unknown = point(0, flags = PointFlags.MOTION_CHANGE or 0x1000)
        assertTrue(unknown.hasMotionChange)
        assertFalse(point(0).hasMotionChange)
    }

    @Test
    fun `可缺省字段默认为 null 且 flags 默认为 0`() {
        val p = point(0)
        assertNull(p.sessionId)
        assertNull(p.taskId)
        assertNull(p.accuracyCm)
        assertNull(p.speedMps)
        assertNull(p.source)
        assertEquals(0, p.flags)
    }

    @Test
    fun `LocationSource 未知协议值归入 UNSPECIFIED 而非抛异常`() {
        assertEquals(LocationSource.GPS, LocationSource.fromProto(1))
        assertEquals(LocationSource.UNSPECIFIED, LocationSource.fromProto(99))
        assertNull(LocationSource.fromStored(99))
        assertEquals(LocationSource.MANUAL, LocationSource.fromStored(4))
    }

    @Test
    fun `Bbox 覆盖全部点且含边界`() {
        val points =
            listOf(
                point(0, latQ = 100, lngQ = 500),
                point(1, latQ = 300, lngQ = 200),
                point(2, latQ = 200, lngQ = 900),
            )
        val bbox = Bbox.of(points)
        assertEquals(Bbox(minLatQ = 100, maxLatQ = 300, minLngQ = 200, maxLngQ = 900), bbox)
        assertTrue(bbox.contains(300, 900))
        assertTrue(bbox.contains(100, 200))
        assertFalse(bbox.contains(301, 900))
        assertFalse(bbox.contains(100, 199))
    }

    @Test
    fun `段草稿校验 STAY 必带质心且时长非负`() {
        val base =
            TraceSegmentDraft(
                segmentId = UUID(0, 1),
                startTsMs = 1_000,
                endTsMs = 2_000,
                kind = SegmentKind.MOVE,
                moveMode = MoveMode.WALK,
                pointCount = 2,
                distanceM = 12.5,
                geometryWkt = "LINESTRING(121.4 31.2, 121.5 31.3)",
                bbox = Bbox(312_000_000, 313_000_000, 1_214_000_000, 1_215_000_000),
            )
        assertEquals(1_000L, base.durationMs)

        val stay =
            base.copy(
                kind = SegmentKind.STAY,
                distanceM = 0.0,
                stayLatQ = 312_500_000,
                stayLngQ = 1_214_500_000,
            )
        assertEquals(SegmentKind.STAY, stay.kind)

        assertTrue(
            runCatching { base.copy(kind = SegmentKind.STAY) }.isFailure,
            "STAY 段缺质心应被拒绝",
        )
        assertTrue(
            runCatching { base.copy(endTsMs = 999) }.isFailure,
            "段结束时间早于开始时间应被拒绝",
        )
        assertTrue(
            runCatching { base.copy(pointCount = 0) }.isFailure,
            "段内点数必须为正",
        )
    }
}
