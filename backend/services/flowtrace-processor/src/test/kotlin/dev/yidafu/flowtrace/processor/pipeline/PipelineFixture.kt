package dev.yidafu.flowtrace.processor.pipeline

import dev.yidafu.flowtrace.common.cache.FlowtraceCache
import dev.yidafu.flowtrace.common.cache.TombstoneStore
import dev.yidafu.flowtrace.common.cache.WatermarkStore
import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.model.SegmentStatus
import dev.yidafu.flowtrace.model.TracePoint
import dev.yidafu.flowtrace.model.TraceSegmentDraft
import dev.yidafu.flowtrace.processor.clean.PointCleaner
import dev.yidafu.flowtrace.processor.config.ProcessorProperties
import dev.yidafu.flowtrace.processor.segment.DeviceStateStore
import dev.yidafu.flowtrace.processor.segment.MoveModeClassifier
import dev.yidafu.flowtrace.processor.segment.OnlineSegmenter
import dev.yidafu.flowtrace.processor.segment.SegmentBuilder
import dev.yidafu.flowtrace.processor.store.RecalcGuard
import dev.yidafu.flowtrace.storage.postgres.repository.DeviceStateLoader
import dev.yidafu.flowtrace.storage.postgres.repository.PointsWriter
import dev.yidafu.flowtrace.storage.postgres.repository.SegmentScope
import dev.yidafu.flowtrace.storage.postgres.repository.TraceBatchRepository
import dev.yidafu.flowtrace.storage.postgres.repository.TraceSegmentRepository
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * 批处理链路的测试脚手架：真实 [BatchPipeline] + 记录型仓储替身。
 *
 * 不 mock 管道内部组件（cleaner/segmenter/store 都用真件），只在**出口**替换：
 * 点位写入、段写入、批状态更新、水位、墓碑。这样断言的是「链路串起来之后的可见副作用」，
 * 而不是「某个方法被调用了几次」。
 *
 * 各个开关（[tombstoneHit] / [watermark] / [recalcLocked] / [tombstoneFailure]）用于把链路
 * 导到不同分支，默认值即「无墓碑、无水位、未加锁」的常规路径。
 */
internal class PipelineFixture(
    val props: ProcessorProperties = ProcessorProperties(),
) {
    /** 实际交给 `trace_point` 的行（已清洗）。 */
    val writtenPoints = mutableListOf<TracePoint>()

    /** 每次 `replaceTail` 的入参摘要。 */
    val segmentWrites = mutableListOf<SegmentWrite>()

    /** `finalizeOpenBefore` 收到的 beforeTsMs（截断恢复路径）。 */
    val finalizedBefore = mutableListOf<Long>()

    /** 被置为 PROCESSED 的 batchId。 */
    val processedBatchIds = mutableListOf<UUID>()

    /** 水位写入（deviceId to ts）。 */
    val watermarkWrites = mutableListOf<Pair<String, Long>>()

    /** 水位读数；null 表示该设备还没有历史点。 */
    var watermark: Long? = null

    var tombstoneHit: Boolean = false

    /** 非空时墓碑查询直接抛异常，用于模拟 Redis/PG 抖动。 */
    var tombstoneFailure: RuntimeException? = null

    /** true 表示该用户正在重算，管道应跳过分段。 */
    var recalcLocked: Boolean = false

    data class SegmentWrite(
        val fromTsMs: Long,
        val drafts: List<TraceSegmentDraft>,
        val status: SegmentStatus,
    )

    private val cache =
        object : FlowtraceCache {
            override fun has(key: String): Boolean = recalcLocked && key.startsWith("recalc:u:")

            override fun <T : Any> get(
                key: String,
                type: Class<T>,
            ): T? = null

            override fun put(
                key: String,
                value: Any,
                ttl: Duration?,
            ) = Unit

            override fun evict(key: String) = Unit

            override fun incrementBy(
                key: String,
                delta: Long,
                ttl: Duration?,
            ): Long = throw UnsupportedOperationException("批处理链路不使用计数器")
        }

    /** 缓存永远是 miss：每个用例都从「设备无历史状态」开始，避免用例间串状态。 */
    private val emptyLoader =
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

    val deviceState = DeviceStateStore(props, emptyLoader)

    private val segmentRepository =
        object : TraceSegmentRepository() {
            override fun replaceTail(
                scope: SegmentScope,
                fromTsMs: Long,
                drafts: List<TraceSegmentDraft>,
                status: SegmentStatus,
            ) {
                segmentWrites += SegmentWrite(fromTsMs, drafts, status)
            }

            override fun finalizeOpenBefore(
                scope: SegmentScope,
                beforeTsMs: Long,
            ): Int {
                finalizedBefore += beforeTsMs
                return 0
            }
        }

    val pipeline =
        BatchPipeline(
            tombstone =
                object : TombstoneStore {
                    override fun has(userId: String): Boolean {
                        tombstoneFailure?.let { throw it }
                        return tombstoneHit
                    }

                    override fun mark(
                        userId: String,
                        jobId: String,
                        ttl: Duration,
                    ) = Unit
                },
            watermark =
                object : WatermarkStore {
                    override fun maxSeenTs(deviceId: String): Long? = watermark

                    override fun writeIfGreater(
                        deviceId: String,
                        tsMs: Long,
                    ) {
                        watermarkWrites += deviceId to tsMs
                    }
                },
            props = props,
            deviceState = deviceState,
            cleaner = PointCleaner(props),
            pointsWriter =
                object : PointsWriter() {
                    override fun batchInsertIgnore(rows: List<TracePoint>): Int {
                        writtenPoints += rows
                        return rows.size
                    }
                },
            recalcGuard = RecalcGuard(cache),
            segmenter = OnlineSegmenter(deviceState, props, segmentRepository, SegmentBuilder(), MoveModeClassifier()),
            batchStatus =
                object : TraceBatchRepository() {
                    override fun markProcessed(
                        batchId: UUID,
                        at: Instant,
                    ): Int {
                        processedBatchIds += batchId
                        return 1
                    }
                },
        )

    /** 组装 Kafka 内部消息（点为绝对坐标）。 */
    fun message(
        points: List<TraceMessage.AbsolutePoint>,
        batchId: String = BATCH_ID,
        deviceId: String = DEVICE_ID,
        userId: String = USER_ID,
        sessionId: String = SESSION_ID,
        taskId: String = TASK_ID,
        channel: Int = 1,
        receivedTsMs: Long = 0L,
    ): TraceMessage.TraceBatchMessage =
        TraceMessage.TraceBatchMessage
            .newBuilder()
            .setBatchId(batchId)
            .setSchemaVersion("1.0")
            .setDeviceId(deviceId)
            .setUserId(userId)
            .setSessionId(sessionId)
            .setTaskId(taskId)
            .setChannel(channel)
            .setReceivedTsMs(receivedTsMs)
            .addAllPoints(points)
            .build()

    companion object {
        const val BATCH_ID = "0190f2a0-0000-7000-8000-0000000000aa"
        const val DEVICE_ID = "dev-1"
        const val USER_ID = "u-1"
        const val SESSION_ID = "s-1"
        const val TASK_ID = "t-1"

        /** 北京附近的一个非零坐标（零岛会被清洗掉）。 */
        const val LAT_Q = 390000000
        const val LNG_Q = 1160000000

        fun point(
            tsMs: Long,
            latQ: Int = LAT_Q,
            lngQ: Int = LNG_Q,
            idx: Int = 0,
            speedMps: Float = 0f,
        ): TraceMessage.AbsolutePoint =
            TraceMessage.AbsolutePoint
                .newBuilder()
                .setTsMs(tsMs)
                .setLatQ(latQ)
                .setLngQ(lngQ)
                .setSpeedMps(speedMps)
                .setIdx(idx)
                .build()
    }
}
