package dev.yidafu.flowtrace.process.segment

import dev.yidafu.flowtrace.process.config.ProcessorProperties
import dev.yidafu.flowtrace.shared.model.TracePoint
import dev.yidafu.flowtrace.shared.persistence.repository.DeviceStateLoader
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * 设备状态恢复测试（08 §8.1）。
 *
 * 缓存 miss 时要靠 3 条 SQL 把状态拼回来，其中 `openTruncated` 是最容易漏的一环：
 * 窗口被 `openSegMaxPoints` 截断后，旧 OPEN 段的起点已不在内存里，
 * 若还按内存首点做 `replaceTail`，旧段既不会被删也不会被封口，库里会留一条永远 OPEN 的残段。
 */
class DeviceStateStoreTest {
    private val props = ProcessorProperties()
    private val loader = FakeLoader()
    private val store = DeviceStateStore(props, loader)

    @Test
    fun `无历史点时返回空状态`() {
        loader.lastKept = null

        val state = store.getOrLoad(DEVICE_ID)

        assertEquals(DEVICE_ID, state.deviceId)
        assertNull(state.lastKept)
        assertNull(state.open)
        assertFalse(state.openTruncated)
    }

    @Test
    fun `恢复 lastKept 与设备身份`() {
        loader.lastKept = point(TS_MS, latQ = 1)
        loader.openStart = null

        val state = store.getOrLoad(DEVICE_ID)

        assertEquals(USER_ID, state.userId)
        assertEquals(SESSION_ID, state.sessionId)
        assertEquals(TASK_ID, state.taskId)
        assertEquals(TS_MS, state.lastKept?.tsMs)
        assertNull(state.open, "没有 OPEN 段时不该造一个空窗口")
    }

    @Test
    fun `窗口首点等于 OPEN 起点时不标记截断`() {
        loader.lastKept = point(TS_MS, latQ = 1)
        loader.openStart = Instant.ofEpochMilli(TS_MS)
        loader.window = listOf(point(TS_MS, latQ = 1), point(TS_MS + 1_000, latQ = 2))

        val state = store.getOrLoad(DEVICE_ID)

        val open = assertNotNull(state.open)
        assertEquals(TS_MS, open.startTsMs)
        assertEquals(2, open.points.size)
        assertFalse(state.openTruncated)
    }

    @Test
    fun `窗口被点数上限截断时标记 openTruncated`() {
        props.openSegMaxPoints = 2
        loader.lastKept = point(TS_MS + 5_000, latQ = 9)
        loader.openStart = Instant.ofEpochMilli(TS_MS)
        // 起点（TS_MS）上的点已被淘汰，内存窗口从更晚的点开始
        loader.window = listOf(point(TS_MS + 1_000, latQ = 2), point(TS_MS + 2_000, latQ = 3))

        val state = store.getOrLoad(DEVICE_ID)

        assertTrue(state.openTruncated, "窗口首点晚于 OPEN 起点说明旧段起点已不在内存，必须走封口路径")
        assertEquals(TS_MS + 1_000, assertNotNull(state.open).startTsMs)
    }

    @Test
    fun `flushAll 把每个设备交给 flusher 并清空缓存`() {
        loader.lastKept = point(TS_MS, latQ = 1)
        store.getOrLoad(DEVICE_ID)
        store.getOrLoad(OTHER_DEVICE)

        val flushed = mutableListOf<String>()
        store.flusher = { flushed += it.deviceId }
        store.flushAll()

        assertEquals(setOf(DEVICE_ID, OTHER_DEVICE), flushed.toSet())
        // 缓存已清空：再取一次必须回源
        val loadsBefore = loader.loadLastKeptCalls
        store.getOrLoad(DEVICE_ID)
        assertEquals(loadsBefore + 1, loader.loadLastKeptCalls)
    }

    // ------------------------------------------------------------------ 脚手架

    private class FakeLoader : DeviceStateLoader() {
        var lastKept: TracePoint? = null
        var openStart: Instant? = null
        var window: List<TracePoint> = emptyList()
        var loadLastKeptCalls: Int = 0

        override fun loadLastKept(deviceId: String): TracePoint? {
            loadLastKeptCalls++
            return lastKept
        }

        override fun loadOpenStartTs(
            userId: String,
            deviceId: String,
        ): Instant? = openStart

        override fun loadWindow(
            userId: String,
            startTs: Instant,
            maxPoints: Int,
        ): List<TracePoint> = window
    }

    private fun point(
        tsMs: Long,
        latQ: Int,
    ) = TracePoint(
        tsMs = tsMs,
        pointId = UUID.randomUUID(),
        userId = USER_ID,
        deviceId = DEVICE_ID,
        batchId = BATCH_ID,
        latQ = latQ,
        lngQ = 1160000000,
        sessionId = SESSION_ID,
        taskId = TASK_ID,
    )

    private companion object {
        const val USER_ID = "u-1"
        const val DEVICE_ID = "dev-1"
        const val OTHER_DEVICE = "dev-2"
        const val SESSION_ID = "s-1"
        const val TASK_ID = "t-1"
        const val TS_MS = 1_700_000_000_000L
        val BATCH_ID: UUID = UUID.fromString("0190f2a0-0000-7000-8000-0000000000dd")
    }
}
