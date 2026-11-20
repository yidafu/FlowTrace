package dev.yidafu.flowtrace.process.listener

import com.google.protobuf.InvalidProtocolBufferException
import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.process.config.ProcessorProperties
import dev.yidafu.flowtrace.process.dlq.DlqSink
import dev.yidafu.flowtrace.process.pipeline.BatchPipeline
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
 * 职责边界：本类只做「解码 → 交给处理管道 → 决定 ack / DLQ / 抛出」，不含清洗、分段等业务。
 */
@Component
class TraceBatchListener(
    private val pipeline: BatchPipeline,
    private val dlqPublisher: DlqSink,
    private val batchStatus: BatchStatusSink,
    private val props: ProcessorProperties,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 单批消费。异常语义（08 §11）：
     * - protobuf 解析失败 / 批头非法：**确定性失败** → 投 DLQ + ack（不能抛出，抛出会被当成
     *   可重试，坏消息永远卡住该分区，而重投一万次结果都一样）；
     * - 管道抛出的可重试异常（DB/Redis 抖动）：不 ack、向上抛，容器按退避重试，
     *   期间该分区阻塞——这是刻意的，绝不能把一次 PG 重启放大成上千条 DLQ；
     * - 单批墙钟超过 `batchTimeoutMs`：按毒丸处理 → 投 DLQ + `markDlq` + ack。
     *   无法中断已经跑起来的 JDBC 调用，所以这里做的是**事后判定**：本批数据已落库（幂等），
     *   投 DLQ 是为了让运维看到「这批判定异常」，重放由 batchId/pointId 幂等兜住。
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
    fun onBatch(
        record: ConsumerRecord<String, ByteArray>,
        ack: Acknowledgment,
    ) {
        val payload = record.value()
        // Kafka 墓碑消息（删除标记）的 value 为 null，不能进 parseFrom
        if (payload == null || payload.isEmpty()) {
            log.warn("收到空消息，丢弃: partition={}, offset={}", record.partition(), record.offset())
            ack.acknowledge()
            return
        }

        val message =
            try {
                TraceMessage.TraceBatchMessage.parseFrom(payload)
            } catch (e: InvalidProtocolBufferException) {
                // 确定性失败：同一串字节重投多少次都一样 → 进 DLQ 留证据，再 ack 放过该分区
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
                ack.acknowledge()
                return
            }

        // 批级基本校验：只拦「消息本身坏了」的情况，字段级校验 upload 已做完（06 §2.3）。
        // batchId 必须能解析成 UUID：管道要按它写 trace_point.batch_id 与台账，解析失败是**确定性**失败，
        // 若放过去会在管道里抛 IllegalArgumentException，被当成可重试 → 一条坏消息把分区永久按住。
        val batchIdText = message.batchId
        val batchId = parseBatchIdOrNull(batchIdText)
        if (batchId == null || message.pointsCount == 0) {
            log.error(
                "非法批次（batchId 为空/非法或无点），投死信: batchId='{}', points={}, offset={}",
                batchIdText,
                message.pointsCount,
                record.offset(),
            )
            dlqPublisher.publish(
                key = record.key(),
                payload = payload,
                errorCode = INVALID_BATCH,
                errorMessage = "batchId 不可用（'$batchIdText'）或无点（points=${message.pointsCount}）",
                batchId = batchIdText.ifBlank { null },
                channel = message.channel,
            )
            batchId?.let { batchStatus.markDlq(it, INVALID_BATCH) }
            ack.acknowledge()
            return
        }

        // 断点①：开始处理。debug 级别——默认 info 下每批只留一行完成摘要（见下方）
        val startNs = System.nanoTime()
        log.debug(
            "processor 开始处理批次 batchId={} deviceId={} userId={} channel={} points={} partition={} offset={} uploadLagMs={}",
            batchIdText,
            message.deviceId,
            message.userId,
            message.channel,
            message.pointsCount,
            record.partition(),
            record.offset(),
            System.currentTimeMillis() - message.receivedTsMs,
        )

        // 断点②：管道异常。不 ack 由容器退避重试，这里只补「哪一批、哪个分区」上下文，
        // 堆栈交给上层 ErrorHandler/日志框架，避免同一次失败打两份栈
        val result =
            try {
                pipeline.process(message)
            } catch (ex: Exception) {
                log.error(
                    "processor 批处理失败（不 ack，按可重试退避） batchId={} deviceId={} userId={} points={} partition={} offset={} costMs={} cause={}",
                    batchIdText,
                    message.deviceId,
                    message.userId,
                    message.pointsCount,
                    record.partition(),
                    record.offset(),
                    (System.nanoTime() - startNs) / NANOS_PER_MILLI,
                    ex.message,
                )
                throw ex
            }
        val costMs = (System.nanoTime() - startNs) / NANOS_PER_MILLI

        // 断点③：每批一行 info（08 §12 字段口径）；>5s 升级为 WARN（07 §5 慢批）
        val summary =
            "batchId=$batchIdText deviceId=${message.deviceId} userId=${message.userId} " +
                "points=${message.pointsCount} kept=${result.kept} dropped=${result.droppedText()} " +
                "late=${result.late} tombstoned=${result.tombstoned} recalcSkipped=${result.recalcSkipped} " +
                "watermarkDegraded=${result.watermarkDegraded} queuedDays=${result.queuedDays} " +
                "segments=${result.segments} costMs=$costMs uploadLagMs=${System.currentTimeMillis() - message.receivedTsMs}"
        if (costMs > SLOW_BATCH_WARN_MS) {
            log.warn("processor 批处理完成（慢批，阈值 {}ms） {}", SLOW_BATCH_WARN_MS, summary)
        } else {
            log.info("processor 批处理完成 {}", summary)
        }

        // 毒丸判定：单批墙钟超过上限（06 §3.7）。这里已经无法中断，只能事后标记并留证据
        if (costMs > props.batchTimeoutMs) {
            log.error(
                "processor 单批处理超过墙钟上限，投死信并标记 DLQ batchId={} deviceId={} costMs={} timeoutMs={}",
                batchIdText,
                message.deviceId,
                costMs,
                props.batchTimeoutMs,
            )
            dlqPublisher.publish(
                key = record.key(),
                payload = payload,
                errorCode = BATCH_TIMEOUT,
                errorMessage = "costMs=$costMs timeoutMs=${props.batchTimeoutMs}",
                batchId = batchIdText,
                channel = message.channel,
            )
            batchStatus.markDlq(batchId, BATCH_TIMEOUT)
        }

        ack.acknowledge()
        log.debug(
            "processor 已 ack batchId={} partition={} offset={}",
            batchIdText,
            record.partition(),
            record.offset(),
        )
    }

    /** batchId 必须是 UUID（SDK 生成 v7）：解析不出来就是坏消息，返回 null 由调用方投 DLQ。 */
    private fun parseBatchIdOrNull(raw: String): UUID? =
        try {
            UUID.fromString(raw)
        } catch (ex: IllegalArgumentException) {
            null
        }

    private companion object {
        /** 单批墙钟告警阈值（07 §5：processor 批处理 > 5s 打 WARN）。 */
        const val SLOW_BATCH_WARN_MS = 5_000L
        const val NANOS_PER_MILLI = 1_000_000L

        /** 不可重试错误码（DLQ 头的 `error.code`，06 §3.7）。 */
        const val INVALID_PROTOBUF = "INVALID_PROTOBUF"
        const val INVALID_BATCH = "PROTOCOL_ERROR"
        const val BATCH_TIMEOUT = "BATCH_TIMEOUT"
    }
}
