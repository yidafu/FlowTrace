package dev.yidafu.flowtrace.process.pipeline

import dev.yidafu.flowtrace.internal.v1.TraceMessage
import java.time.LocalDate
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * 批元信息测试（08 §6）。
 *
 * [BatchMeta.of] 同时是迟到判定的输入（批内首末 ts）与身份列/批号的权威来源：
 * `DeviceState` 是跨批缓存，session/task 可能还停留在上一批，**批头才是本批的权威值**
 * （身份覆盖行为在 `PointCleanerTest` 里断言，这里只验证投影本身是否正确）。
 */
class BatchMetaTest {
    @Test
    fun `批元信息取批内首末点与批头身份`() {
        val message =
            TraceMessage.TraceBatchMessage
                .newBuilder()
                .setBatchId("0190f2a0-0000-7000-8000-0000000000cc")
                .setDeviceId("dev-9")
                .setUserId("u-9")
                .setSessionId("s-9")
                .setTaskId("t-9")
                .setChannel(2)
                .setReceivedTsMs(777)
                .addPoints(point(5_000))
                .addPoints(point(9_000))
                .build()

        val meta = BatchMeta.of(message)

        assertEquals("u-9", meta.userId)
        assertEquals("dev-9", meta.deviceId)
        assertEquals("s-9", meta.sessionId)
        assertEquals("t-9", meta.taskId)
        assertEquals(2, meta.channel)
        assertEquals(777, meta.receivedTsMs)
        assertEquals(5_000, meta.minTsMs)
        assertEquals(9_000, meta.maxTsMs)
        // 批号同时给「uuid 形态（台账/列值）」与「原样字符串（pointId 派生）」，两者不能各自解析一次
        assertEquals(UUID.fromString("0190f2a0-0000-7000-8000-0000000000cc"), meta.batchId)
        assertEquals("0190f2a0-0000-7000-8000-0000000000cc", meta.batchIdText)
    }

    @Test
    fun `跨 UTC 日的批登记两天`() {
        // 2026-11-19T23:59:00Z 与 2026-11-20T00:01:00Z：北京时区 11-20 07:59 / 08:01，但在 UTC 上跨日
        val start = 1_795_132_740_000L
        val meta = metaOf(start, start + 120_000)

        assertEquals(listOf(LocalDate.of(2026, 11, 19), LocalDate.of(2026, 11, 20)), meta.coveredUtcDays())
    }

    @Test
    fun `单点批只登记一天`() {
        val meta = metaOf(1_000, 1_000)

        assertEquals(listOf(LocalDate.of(1970, 1, 1)), meta.coveredUtcDays())
    }

    @Test
    fun `超长跨度按上限截断`() {
        // 端点越界只可能来自脏数据：截断好过为一个坏批写上千条 dirty_day
        val meta = metaOf(0, 400L * 86_400_000)

        assertEquals(BatchMeta.MAX_COVERED_DAYS, meta.coveredUtcDays().size)
    }

    private fun metaOf(
        minTsMs: Long,
        maxTsMs: Long,
    ) = BatchMeta(
        batchId = UUID.fromString("0190f2a0-0000-7000-8000-0000000000dd"),
        batchIdText = "0190f2a0-0000-7000-8000-0000000000dd",
        userId = "u-utc",
        deviceId = "dev-utc",
        sessionId = "",
        taskId = "",
        channel = 1,
        receivedTsMs = 0,
        minTsMs = minTsMs,
        maxTsMs = maxTsMs,
    )

    private fun point(tsMs: Long) =
        TraceMessage.AbsolutePoint
            .newBuilder()
            .setTsMs(tsMs)
            .setLatQ(390000000)
            .setLngQ(1160000000)
            .build()
}
