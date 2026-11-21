package dev.yidafu.flowtrace.process.listener

import com.google.protobuf.InvalidProtocolBufferException
import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.process.clean.DropReason
import dev.yidafu.flowtrace.process.config.ProcessorProperties
import dev.yidafu.flowtrace.process.dlq.DlqSink
import dev.yidafu.flowtrace.process.metrics.ProcessorMetrics
import dev.yidafu.flowtrace.process.pipeline.BatchPipeline
import dev.yidafu.flowtrace.process.pipeline.toCountText
import dev.yidafu.flowtrace.process.store.BatchStatusSink
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import java.util.UUID

/**
 * 轨迹批消费入口（06 §3.1 / 08 §4、§11）。
 *
 * 监听链路要「对上」的只有四件事，缺一件就收不到消息：
 * 1. `@KafkaListener` 的 `containerFactory` 指向 `ProcessorKafkaConfig` 里的
 *    `traceProcessorContainerFactory`（bean 名即方法名，改名即断链）；
 * 2. `group.id` 已在 consumer factory 里设（`ProcessorProperties.consumerGroup`），
 *    注解上不再重复写——两处都写容易出现「改了一个以为生效」的坑；
 * 3. value 反序列化器必须是 `ByteArrayDeserializer`：消息体是 protobuf 原始字节，
 *    在监听器内自己 `parseFrom`，不能指望 JSON/字符串反序列化器；
 * 4. `AckMode.MANUAL`：容器不自动提交，由本类处理成功后显式 `acknowledge()`。
 *    Boot 的 `KafkaAutoConfiguration` 已注册 `KafkaListenerAnnotationBeanPostProcessor`，
 *    不需要额外 `@EnableKafka`。
 *
 * **批式监听（08 §4「poll 攒 N 批合并写」，2026-11-21）**：方法签名收
 * `List<ConsumerRecord>`，Spring Kafka 按 batch listener 装配——一次 poll（上限
 * `poll-records`）的记录交给 [BatchPipeline.processAll] 合成一个 cycle 处理，
 * 末尾一次性 `acknowledge()` 提交整个 cycle 的 offset。
 *
 * 职责边界：本类只做「解码 → 交给处理管道 → 决定 ack / DLQ / 抛出」，不含清洗、分段等业务。
 */
@Component
class TraceBatchListener(
    private val pipeline: BatchPipeline,
    private val dlqPublisher: DlqSink,
    private val batchStatus: BatchStatusSink,
    private val props: ProcessorProperties,
    private val metrics: ProcessorMetrics,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /** 分段计时包装（与 [BatchPipeline] 同口径）：异常也计入耗时。 */
    private inline fun <T> stage(
        name: String,
        block: () -> T,
    ): T {
        val start = System.nanoTime()
        try {
            return block()
        } finally {
            metrics.stage(name, System.nanoTime() - start)
        }
    }

    /**
     * 一个 poll cycle 的消费。异常语义（08 §11）：
     * - protobuf 解析失败 / 批头非法：**确定性失败** → 投 DLQ + 继续处理同 cycle 的好消息
     *   （不能抛出，抛出会被当成可重试，坏消息永远卡住该分区，而重投一万次结果都一样）；
     * - 管道抛出的可重试异常（DB/Redis 抖动）：不 ack、向上抛，容器按退避重试，**整个 cycle
     *   原地重放**（幂等吸收放大）——期间该分区阻塞，这是刻意的，绝不能把一次 PG 重启放大成上千条 DLQ；
     * - cycle 墙钟超过 `batchTimeoutMs`：按毒丸处理 → 整 cycle 投 DLQ + `markDlq` + ack。
     *   无法中断已经跑起来的 JDBC 调用，所以这里做的是**事后判定**：本 cycle 数据已落库（幂等），
     *   投 DLQ 是为了让运维看到「这段判定异常」，重放由 batchId/pointId 幂等兜住。
     *
     * [ack] 用 `Acknowledgment` 而非手工提交 offset：`AckMode.MANUAL` 下 `acknowledge()`
     * 只把 offset 入队，下一次 poll 才真正提交（`MANUAL_IMMEDIATE` 才逐条同步提交）。
     * 进程在「已 ack、未提交」之间崩溃会导致重放，管道全链路幂等（pointId / segmentId
     * 确定性派生）可以吸收，所以这里选更省 IO 的 MANUAL。
     */
    @KafkaListener(
        topics = [$$"${flowtrace.upload.topic:trace.batch.v1}"],
        containerFactory = "traceProcessorContainerFactory",
    )
    fun onBatches(
        records: List<ConsumerRecord<String, ByteArray>>,
        ack: Acknowledgment,
    ) {
        val startNs = System.nanoTime()

        // 断点①：解码 + 批级校验。坏消息各自投 DLQ 后**继续处理同 cycle 的好消息**——
        // 一条毒丸不能拖累同一 poll 里的正常批（单记录时代它们本来也是各走各的）。
        // 批级基本校验只拦「消息本身坏了」的情况，字段级校验 upload 已做完（06 §2.3）：
        // batchId 必须能解析成 UUID（管道要按它写 trace_point.batch_id 与台账），
        // 解析失败是**确定性**失败，放过去会在管道里抛 IllegalArgumentException。
        val cycle =
            stage("decode") {
                val decoded = ArrayList<CycleRecord>(records.size)
                for (record in records) {
                    val payload = record.value()
                    // Kafka 墓碑消息（删除标记）的 value 为 null，不能进 parseFrom
                    if (payload == null || payload.isEmpty()) {
                        log.warn("收到空消息，丢弃: partition={}, offset={}", record.partition(), record.offset())
                        continue
                    }
                    val message =
                        try {
                            TraceMessage.TraceBatchMessage.parseFrom(payload)
                        } catch (e: InvalidProtocolBufferException) {
                            log.error(
                                "protobuf 解析失败，投死信: partition={}, offset={}, bytes={}",
                                record.partition(),
                                record.offset(),
                                payload.size,
                                e,
                            )
                            dlqPublisher.publish(
                                key = record.key(),
                                payload = payload,
                                errorCode = INVALID_PROTOBUF,
                                errorMessage = e.message,
                            )
                            continue
                        }
                    val batchId = parseBatchIdOrNull(message.batchId)
                    if (batchId == null || message.pointsCount == 0) {
                        log.error(
                            "非法批次（batchId 为空/非法或无点），投死信: batchId='{}', points={}, offset={}",
                            message.batchId,
                            message.pointsCount,
                            record.offset(),
                        )
                        dlqPublisher.publish(
                            key = record.key(),
                            payload = payload,
                            errorCode = INVALID_BATCH,
                            errorMessage = "batchId 不可用（'${message.batchId}'）或无点（points=${message.pointsCount}）",
                            batchId = message.batchId.ifBlank { null },
                            channel = message.channel,
                        )
                        batchId?.let { batchStatus.markDlq(it, INVALID_BATCH) }
                        continue
                    }
                    decoded += CycleRecord(record, message, batchId, message.batchId)
                }
                decoded
            }
        if (cycle.isEmpty()) {
            ack.acknowledge()
            return
        }
        log.debug(
            "processor 开始处理 cycle batches={} firstPartition={} firstOffset={} uploadLagMs={}",
            cycle.size,
            cycle.first().record.partition(),
            cycle.first().record.offset(),
            System.currentTimeMillis() - cycle.minOf { it.message.receivedTsMs },
        )

        // 断点②：管道异常。不 ack 由容器退避重试并重放整个 cycle，这里只补「哪些批、哪个分区」上下文，
        // 堆栈交给上层 ErrorHandler/日志框架，避免同一次失败打两份栈
        val results =
            try {
                pipeline.processAll(cycle.map { it.message })
            } catch (ex: Exception) {
                log.error(
                    "processor 批处理失败（不 ack，按可重试退避重放整个 cycle） batches={} points={} partition={} firstOffset={} costMs={} cause={}",
                    cycle.size,
                    cycle.sumOf { it.message.pointsCount },
                    cycle.first().record.partition(),
                    cycle.first().record.offset(),
                    (System.nanoTime() - startNs) / NANOS_PER_MILLI,
                    ex.message,
                )
                throw ex
            }
        val costMs = (System.nanoTime() - startNs) / NANOS_PER_MILLI

        // 指标断点：按 result 拆「入库」与「按原因丢弃」。放在日志之前——日志级别关掉时指标也必须存在。
        // 每批只在这里记一次，管道本身保持纯逻辑（不依赖 Micrometer），BatchResult 就是它的计数契约。
        val droppedMerged = LinkedHashMap<DropReason, Int>()
        results.forEach { result ->
            metrics.pointsInserted(result.kept)
            result.dropped.forEach { (reason, count) -> droppedMerged.merge(reason, count) { prev, next -> prev + next } }
            metrics.pointsDropped(result.dropped)
        }

        // 断点③：每 cycle 一行 info（08 §12 字段口径）；>5s 升级为 WARN（07 §5 慢批）。
        // 摘要是 cycle 级聚合——逐批明细在管道 debug 日志里。
        val summary =
            "batches=${results.size} points=${results.sumOf { it.pointsIn }} kept=${results.sumOf { it.kept }} " +
                "dropped=${droppedMerged.toCountText()} late=${results.count { it.late }} " +
                "tombstoned=${results.count { it.tombstoned }} recalcSkipped=${results.count { it.recalcSkipped }} " +
                "watermarkDegraded=${results.any { it.watermarkDegraded }} queuedDays=${results.sumOf { it.queuedDays }} " +
                "segments=${results.sumOf { it.segments }} costMs=$costMs " +
                "uploadLagMs=${System.currentTimeMillis() - cycle.minOf { it.message.receivedTsMs }}"
        if (costMs > SLOW_BATCH_WARN_MS) {
            log.warn("processor 批处理完成（慢 cycle，阈值 {}ms） {}", SLOW_BATCH_WARN_MS, summary)
        } else {
            log.info("processor 批处理完成 {}", summary)
        }

        // 毒丸判定：cycle 墙钟超过上限（06 §3.7）。合批后无法归因到单批，而 cycle 数据已落库（幂等），
        // 整 cycle 投 DLQ 留证据让运维看到；正常运行 cycle 墙钟在百毫秒级，60s 上限余量充足
        if (costMs > props.batchTimeoutMs) {
            log.error(
                "processor 单 cycle 处理超过墙钟上限，整 cycle 投死信并标记 DLQ batches={} costMs={} timeoutMs={}",
                cycle.size,
                costMs,
                props.batchTimeoutMs,
            )
            cycle.forEach { item ->
                dlqPublisher.publish(
                    key = item.record.key(),
                    payload = item.record.value(),
                    errorCode = BATCH_TIMEOUT,
                    errorMessage = "costMs=$costMs timeoutMs=${props.batchTimeoutMs}",
                    batchId = item.batchIdText,
                    channel = item.message.channel,
                )
                batchStatus.markDlq(item.batchId, BATCH_TIMEOUT)
            }
        }

        ack.acknowledge()
        log.debug(
            "processor 已 ack batches={} partition={} firstOffset={} lastOffset={}",
            cycle.size,
            cycle.first().record.partition(),
            cycle.first().record.offset(),
            cycle.last().record.offset(),
        )
    }

    /** batchId 必须是 UUID（SDK 生成 v7）：解析不出来就是坏消息，返回 null 由调用方投 DLQ。 */
    private fun parseBatchIdOrNull(raw: String): UUID? =
        try {
            UUID.fromString(raw)
        } catch (ex: IllegalArgumentException) {
            null
        }

    /** 一个 poll cycle 内已通过校验的批：record 保留引用，供 cycle 级毒丸判定按原样投 DLQ。 */
    private data class CycleRecord(
        val record: ConsumerRecord<String, ByteArray>,
        val message: TraceMessage.TraceBatchMessage,
        val batchId: UUID,
        /** 批号原样字符串：DLQ 头必须回填原值（解析再 toString 会改变大小写）。 */
        val batchIdText: String,
    )

    private companion object {
        /** cycle 墙钟告警阈值（07 §5：processor 批处理 > 5s 打 WARN）。 */
        const val SLOW_BATCH_WARN_MS = 5_000L
        const val NANOS_PER_MILLI = 1_000_000L

        /** 不可重试错误码（DLQ 头的 `error.code`，06 §3.7）。 */
        const val INVALID_PROTOBUF = "INVALID_PROTOBUF"
        const val INVALID_BATCH = "PROTOCOL_ERROR"
        const val BATCH_TIMEOUT = "BATCH_TIMEOUT"
    }
}
