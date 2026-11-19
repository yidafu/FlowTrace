package dev.yidafu.flowtrace.processor.pipeline

import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.model.TracePoint
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * 批元信息与「点 → 库行」映射测试（08 §6）。
 *
 * [BatchMeta.of] 是迟到判定的输入（批内首末 ts），[TracePoint.toRow] 是身份列的权威来源：
 * `DeviceState` 是跨批缓存，session/task 可能还停留在上一批，**批头才是本批的权威值**。
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
    }

    @Test
    fun `落库行用批头身份覆盖设备缓存里的旧值`() {
        val point =
            TracePoint(
                tsMs = 1_000,
                pointId = UUID.randomUUID(),
                userId = "stale-user",
                deviceId = "stale-device",
                batchId = UUID.randomUUID(),
                latQ = 390000000,
                lngQ = 1160000000,
                sessionId = "stale-session",
                taskId = "stale-task",
            )
        val meta = meta(sessionId = "s-new", taskId = "t-new")

        val row = point.toRow(meta)

        assertEquals("u-new", row.userId)
        assertEquals("dev-new", row.deviceId)
        assertEquals("s-new", row.sessionId)
        assertEquals("t-new", row.taskId)
    }

    @Test
    fun `空串 session 与 task 收敛为 null`() {
        val point = basePoint()

        val row = point.toRow(meta(sessionId = "", taskId = ""))

        assertNull(row.sessionId, "proto3 string 缺省是空串，库列应存 NULL，避免查询侧写两套判空")
        assertNull(row.taskId)
    }

    @Test
    fun `非空 session 与 task 原样保留`() {
        val row = basePoint().toRow(meta(sessionId = "s-1", taskId = "t-1"))

        assertEquals("s-1", row.sessionId)
        assertEquals("t-1", row.taskId)
    }

    private fun basePoint() =
        TracePoint(
            tsMs = 1_000,
            pointId = UUID.randomUUID(),
            userId = "u-new",
            deviceId = "dev-new",
            batchId = UUID.randomUUID(),
            latQ = 390000000,
            lngQ = 1160000000,
        )

    private fun meta(
        sessionId: String,
        taskId: String,
    ) = BatchMeta(
        userId = "u-new",
        deviceId = "dev-new",
        sessionId = sessionId,
        taskId = taskId,
        channel = 1,
        receivedTsMs = 0,
        minTsMs = 1_000,
        maxTsMs = 1_000,
    )

    private fun point(tsMs: Long) =
        TraceMessage.AbsolutePoint
            .newBuilder()
            .setTsMs(tsMs)
            .setLatQ(390000000)
            .setLngQ(1160000000)
            .build()
}
