package dev.yidafu.flowtrace.process.segment

import dev.yidafu.flowtrace.process.config.ProcessorProperties
import dev.yidafu.flowtrace.shared.model.SegmentStatus
import dev.yidafu.flowtrace.shared.model.TracePoint
import dev.yidafu.flowtrace.shared.model.TraceSegmentDraft
import dev.yidafu.flowtrace.shared.persistence.repository.DeviceStateLoader
import dev.yidafu.flowtrace.shared.persistence.repository.SegmentScope
import dev.yidafu.flowtrace.shared.persistence.repository.TraceSegmentRepository
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * 回归：`OnlineSegmenter` 必须把 `DeviceStateStore.flusher` 装配上（08 §8.1）。
 *
 * 未装配时淘汰/停机只会打一条 error，内存里的 OPEN 窗口直接蒸发——那段轨迹在库里永远缺一截。
 * 用例把缓存容量压到 1，塞两台设备触发 LRU 淘汰，断言被淘汰设备的窗口以 **OPEN**（可续写，
 * 不是 FINALIZED）状态写进了仓储。
 */
class OnlineSegmenterFlusherWiringTest {
    @Test
    fun `设备状态被淘汰时把 OPEN 段按 OPEN 状态落库`() {
        val props =
            ProcessorProperties().apply {
                deviceStateCacheSize = 1
                openSegMaxPoints = 10
            }
        val loader =
            object : DeviceStateLoader() {
                override fun loadLastKept(deviceId: String): TracePoint = point(deviceId, TS_MS)

                override fun loadOpenStartTs(
                    userId: String,
                    deviceId: String,
                ): Instant = Instant.ofEpochMilli(TS_MS)

                override fun loadWindow(
                    userId: String,
                    startTs: Instant,
                    maxPoints: Int,
                ): List<TracePoint> = listOf(point(DEVICE_A, TS_MS))
            }
        val recorded = mutableListOf<Pair<String, SegmentStatus>>()
        val writer =
            object : TraceSegmentRepository() {
                override fun replaceTail(
                    scope: SegmentScope,
                    fromTsMs: Long,
                    drafts: List<TraceSegmentDraft>,
                    status: SegmentStatus,
                ) {
                    recorded += scope.deviceId to status
                }
            }

        val store = DeviceStateStore(props, loader)
        OnlineSegmenter(store, props, writer, SegmentBuilder(), MoveModeClassifier())

        // 容量为 1：加载 B 必然把 A 挤出去，触发 flusher
        store.getOrLoad(DEVICE_A)
        store.getOrLoad(DEVICE_B)

        assertEquals(1, recorded.size, "淘汰时未落库 OPEN 段，flusher 可能未装配")
        assertEquals(DEVICE_A, recorded.single().first)
        assertEquals(SegmentStatus.OPEN, recorded.single().second, "淘汰落库必须是 OPEN（可续写），不能是 FINALIZED")
        assertTrue(store.getOrLoad(DEVICE_A).open != null)
    }

    private fun point(
        deviceId: String,
        tsMs: Long,
    ): TracePoint =
        TracePoint(
            tsMs = tsMs,
            pointId = UUID.randomUUID(),
            userId = USER_ID,
            deviceId = deviceId,
            batchId = UUID.randomUUID(),
            latQ = 390000000,
            lngQ = 1160000000,
        )

    private companion object {
        const val USER_ID = "u-1"
        const val DEVICE_A = "dev-a"
        const val DEVICE_B = "dev-b"
        const val TS_MS = 1_000L
    }
}
