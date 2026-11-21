package dev.yidafu.flowtrace.process.pipeline

import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.process.clean.PointCleaner
import dev.yidafu.flowtrace.process.config.ProcessorProperties
import dev.yidafu.flowtrace.process.metrics.ProcessorMetrics
import dev.yidafu.flowtrace.process.segment.DeviceStateStore
import dev.yidafu.flowtrace.process.segment.MoveModeClassifier
import dev.yidafu.flowtrace.process.segment.OnlineSegmenter
import dev.yidafu.flowtrace.process.segment.SegmentBuilder
import dev.yidafu.flowtrace.process.store.BatchStatusSink
import dev.yidafu.flowtrace.process.store.RecalcGuard
import dev.yidafu.flowtrace.shared.cache.RecalcLockStore
import dev.yidafu.flowtrace.shared.cache.TombstoneStore
import dev.yidafu.flowtrace.shared.cache.WatermarkStore
import dev.yidafu.flowtrace.shared.model.DirtyDayReason
import dev.yidafu.flowtrace.shared.model.SegmentStatus
import dev.yidafu.flowtrace.shared.model.TracePoint
import dev.yidafu.flowtrace.shared.model.TraceSegmentDraft
import dev.yidafu.flowtrace.shared.persistence.repository.DeviceStateLoader
import dev.yidafu.flowtrace.shared.persistence.repository.DirtyDayRepository
import dev.yidafu.flowtrace.shared.persistence.repository.PointsWriter
import dev.yidafu.flowtrace.shared.persistence.repository.SegmentScope
import dev.yidafu.flowtrace.shared.persistence.repository.TracePointRepository
import dev.yidafu.flowtrace.shared.persistence.repository.TraceSegmentRepository
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.util.UUID

/**
 * 批处理链路的测试脚手架：真实 [BatchPipeline] + 记录型仓储替身。
 *
 * 不 mock 管道内部组件（cleaner/segmenter/store 都用真件），只在**出口**替换：
 * 点位写入、段写入、批状态更新、水位、墓碑、dirty_day 入队。这样断言的是
 * 「链路串起来之后的可见副作用」，而不是「某个方法被调用了几次」。
 *
 * 各个开关（[tombstoneHit] / [watermark] / [recalcLocked] / [tombstoneFailure] /
 * [watermarkFailure]）用于把链路导到不同分支，默认值即「无墓碑、无水位、未加锁」的常规路径。
 *
 * 批状态用**同步替身**（[BatchStatusSink]）：生产实现是异步队列，
 * 若在用例里等后台线程，断言会变成隐式 flaky。
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

    /** `finalizeOpenRange` 收到的 (fromTsMsInclusive, toTsMsExclusive)：闭合 flush 的前缀翻状态。 */
    val finalizedRanges = mutableListOf<Pair<Long, Long>>()

    /** 被置为 PROCESSED 的 batchId。 */
    val processedBatchIds = mutableListOf<UUID>()

    /** 被置为 FAILED 的 (batchId, errorCode)。 */
    val failedBatches = mutableListOf<Pair<UUID, String>>()

    /** 被置为 DLQ 的 (batchId, errorCode)。 */
    val dlqBatches = mutableListOf<Pair<UUID, String>>()

    /** `dirty_day` 入队记录：userId to 覆盖的 UTC 日。 */
    val enqueuedDays = mutableListOf<Pair<String, List<LocalDate>>>()

    /** 水位写入（deviceId to ts）。 */
    val watermarkWrites = mutableListOf<Pair<String, Long>>()

    /** 水位读数；null 表示该设备还没有历史点。 */
    var watermark: Long? = null

    var tombstoneHit: Boolean = false

    /** 按用户命中的墓碑（合批用例区分「cycle 内谁被删、谁正常」）；与 [tombstoneHit] 取并集。 */
    var tombstoneUsers: Set<String> = emptySet()

    /** 非空时墓碑查询直接抛异常，用于模拟 Redis/PG 抖动。 */
    var tombstoneFailure: RuntimeException? = null

    /** 非空时水位读写直接抛异常，用于验证「降级为不判迟到、不推进水位」。 */
    var watermarkFailure: RuntimeException? = null

    /** true 表示该用户正在重算，管道应跳过分段。 */
    var recalcLocked: Boolean = false

    /** 非空时重算锁查询抛异常：按 08 §7 应视为「已加锁」。 */
    var recalcLockFailure: RuntimeException? = null

    data class SegmentWrite(
        val fromTsMs: Long,
        val drafts: List<TraceSegmentDraft>,
        val status: SegmentStatus,
    )

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

            override fun finalizeOpenRange(
                scope: SegmentScope,
                fromTsMsInclusive: Long,
                toTsMsExclusive: Long,
            ): Int {
                finalizedRanges += fromTsMsInclusive to toTsMsExclusive
                return 0
            }
        }

    private val recalcLockStore =
        object : RecalcLockStore {
            override fun tryLock(
                userId: String,
                ttl: Duration,
            ): Boolean {
                recalcLockFailure?.let { throw it }
                return !recalcLocked
            }

            override fun isLocked(userId: String): Boolean {
                recalcLockFailure?.let { throw it }
                return recalcLocked
            }

            override fun filterLocked(userIds: Collection<String>): Set<String> {
                recalcLockFailure?.let { throw it }
                return if (recalcLocked) userIds.toSet() else emptySet()
            }

            override fun unlock(userId: String) = Unit
        }

    val pipeline =
        BatchPipeline(
            metrics = ProcessorMetrics(SimpleMeterRegistry()),
            tombstone =
                object : TombstoneStore {
                    override fun has(userId: String): Boolean {
                        tombstoneFailure?.let { throw it }
                        return tombstoneHit || userId in tombstoneUsers
                    }

                    override fun filterTombstoned(userIds: Collection<String>): Set<String> {
                        tombstoneFailure?.let { throw it }
                        return userIds.filter { tombstoneHit || it in tombstoneUsers }.toSet()
                    }

                    override fun mark(
                        userId: String,
                        jobId: String,
                        ttl: Duration,
                    ) = Unit
                },
            watermark =
                object : WatermarkStore {
                    override fun maxSeenTs(deviceId: String): Long? {
                        watermarkFailure?.let { throw it }
                        return watermark
                    }

                    override fun maxSeenTsBatch(deviceIds: Collection<String>): Map<String, Long> {
                        watermarkFailure?.let { throw it }
                        return if (watermark != null) deviceIds.associateWith { watermark!! } else emptyMap()
                    }

                    override fun writeIfGreater(
                        deviceId: String,
                        tsMs: Long,
                    ) {
                        watermarkFailure?.let { throw it }
                        watermarkWrites += deviceId to tsMs
                    }

                    override fun writeIfGreaterBatch(entries: Map<String, Long>) {
                        watermarkFailure?.let { throw it }
                        entries.forEach { (deviceId, tsMs) -> watermarkWrites += deviceId to tsMs }
                    }
                },
            props = props,
            deviceState = deviceState,
            cleaner = PointCleaner(props),
            pointsWriter =
                object : PointsWriter(TracePointRepository()) {
                    override fun batchInsertIgnore(rows: List<TracePoint>): Int {
                        writtenPoints += rows
                        return rows.size
                    }
                },
            recalcGuard = RecalcGuard(recalcLockStore, props),
            segmenter = OnlineSegmenter(deviceState, props, segmentRepository, SegmentBuilder(), MoveModeClassifier()),
            batchStatus =
                object : BatchStatusSink {
                    override fun markProcessed(batchId: UUID) {
                        processedBatchIds += batchId
                    }

                    override fun markFailed(
                        batchId: UUID,
                        errorCode: String,
                    ) {
                        failedBatches += batchId to errorCode
                    }

                    override fun markDlq(
                        batchId: UUID,
                        errorCode: String,
                    ) {
                        dlqBatches += batchId to errorCode
                    }
                },
            dirtyDay =
                object : DirtyDayRepository() {
                    override fun enqueue(
                        userId: String,
                        days: Collection<LocalDate>,
                        reason: DirtyDayReason,
                        now: Instant,
                    ): Int {
                        enqueuedDays += userId to days.toList()
                        return days.size
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
