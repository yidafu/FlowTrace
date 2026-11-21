package dev.yidafu.flowtrace.process.listener

import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.process.config.ProcessorProperties
import dev.yidafu.flowtrace.process.dlq.DlqSink
import dev.yidafu.flowtrace.process.metrics.ProcessorMetrics
import dev.yidafu.flowtrace.process.pipeline.PipelineFixture
import dev.yidafu.flowtrace.process.store.BatchStatusSink
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.support.Acknowledgment
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * 消费入口测试（06 §3.1，08 §4、§11）：只验证「解码 → 交给管道 → ack / 抛出」这条责任链。
 *
 * 合批后监听器一次收到一个 poll cycle（`List<ConsumerRecord>`），错误语义与单记录时代逐条对应：
 * - 确定性失败（空消息 / 解析失败 / 批头非法）必须 **投 DLQ 后继续处理同 cycle 的好消息**，
 *   末尾一次性 ack——抛出会让坏消息永远卡住分区；
 * - 可重试失败（管道抛异常）必须 **不 ack 且向上抛**——吞掉就等于把一次 PG 重启放大成上千条丢批，
 *   重放按 cycle 整体进行（幂等吸收）。
 */
class TraceBatchListenerTest {
    @Test
    fun `Kafka 墓碑消息（value 为 null）直接 ack 丢弃`() {
        val fixture = fixture()
        val ack = RecordingAck()

        listener(fixture).onBatches(listOf(record(null)), ack)

        assertEquals(1, ack.count)
        assertTrue(fixture.processedBatchIds.isEmpty(), "空消息不该进管道")
    }

    @Test
    fun `空字节消息直接 ack 丢弃`() {
        val fixture = fixture()
        val ack = RecordingAck()

        listener(fixture).onBatches(listOf(record(ByteArray(0))), ack)

        assertEquals(1, ack.count)
        assertTrue(fixture.processedBatchIds.isEmpty())
    }

    @Test
    fun `protobuf 解析失败投死信并 ack`() {
        val fixture = fixture()
        val ack = RecordingAck()
        val dlq = RecordingDlq()

        listener(fixture, dlq = dlq).onBatches(listOf(record(byteArrayOf(0xFF.toByte(), 0xFF.toByte(), 0xFF.toByte()))), ack)

        assertEquals(1, ack.count, "确定性失败必须 ack，否则该分区被坏消息永久卡住")
        assertEquals("INVALID_PROTOBUF", dlq.published.single().errorCode, "坏字节要留证据，不能只打日志")
        assertEquals(PipelineFixture.DEVICE_ID, dlq.published.single().key, "DLQ 分区键沿用 deviceId，保持可回溯")
        assertTrue(fixture.processedBatchIds.isEmpty())
    }

    @Test
    fun `batchId 为空投死信但不标记台账`() {
        val fixture = fixture()
        val ack = RecordingAck()
        val payload =
            TraceMessage.TraceBatchMessage
                .newBuilder()
                .setBatchId("")
                .setDeviceId(PipelineFixture.DEVICE_ID)
                .setUserId(PipelineFixture.USER_ID)
                .addPoints(PipelineFixture.point(1_000))
                .build()
                .toByteArray()

        val dlq = RecordingDlq()
        val status = RecordingStatus()
        listener(fixture, dlq = dlq, status = status).onBatches(listOf(record(payload)), ack)

        assertEquals(1, ack.count)
        assertEquals("PROTOCOL_ERROR", dlq.published.single().errorCode)
        assertTrue(status.dlqMarked.isEmpty(), "batchId 解析不出来，台账无从标记")
        assertTrue(fixture.processedBatchIds.isEmpty())
    }

    @Test
    fun `无点的批投死信并标记台账`() {
        val fixture = fixture()
        val ack = RecordingAck()
        val payload =
            TraceMessage.TraceBatchMessage
                .newBuilder()
                .setBatchId(PipelineFixture.BATCH_ID)
                .setDeviceId(PipelineFixture.DEVICE_ID)
                .setUserId(PipelineFixture.USER_ID)
                .build()
                .toByteArray()

        val dlq = RecordingDlq()
        val status = RecordingStatus()
        listener(fixture, dlq = dlq, status = status).onBatches(listOf(record(payload)), ack)

        assertEquals(1, ack.count)
        assertEquals("PROTOCOL_ERROR", dlq.published.single().errorCode)
        assertEquals(listOf(UUID.fromString(PipelineFixture.BATCH_ID) to "PROTOCOL_ERROR"), status.dlqMarked)
        assertTrue(fixture.processedBatchIds.isEmpty())
    }

    @Test
    fun `batchId 不是 UUID 时投死信并 ack`() {
        val fixture = fixture()
        val ack = RecordingAck()
        val payload =
            TraceMessage.TraceBatchMessage
                .newBuilder()
                .setBatchId("not-a-uuid")
                .setDeviceId(PipelineFixture.DEVICE_ID)
                .setUserId(PipelineFixture.USER_ID)
                .addPoints(PipelineFixture.point(1_000))
                .build()
                .toByteArray()

        val dlq = RecordingDlq()
        val status = RecordingStatus()
        listener(fixture, dlq = dlq, status = status).onBatches(listOf(record(payload)), ack)

        assertEquals(1, ack.count, "坏批号是确定性失败，抛出会让分区被同一条消息永久按住")
        assertEquals("PROTOCOL_ERROR", dlq.published.single().errorCode)
        assertEquals("not-a-uuid", dlq.published.single().batchId, "原样保留批号，便于人工定位是哪个端发出的")
        assertTrue(status.dlqMarked.isEmpty(), "解析不出 UUID，台账无从标记")
        assertTrue(fixture.processedBatchIds.isEmpty())
    }

    @Test
    fun `正常批处理成功后 ack 一次且不投死信`() {
        val fixture = fixture()
        val ack = RecordingAck()
        val payload =
            fixture
                .message(
                    listOf(
                        PipelineFixture.point(1_000),
                        PipelineFixture.point(2_000, lngQ = PipelineFixture.LNG_Q + 10),
                    ),
                ).toByteArray()

        val dlq = RecordingDlq()
        listener(fixture, dlq = dlq).onBatches(listOf(record(payload)), ack)

        assertEquals(1, ack.count, "一个 cycle 只 ack 一次")
        assertTrue(dlq.published.isEmpty(), "正常批不该进死信")
        assertEquals(1, fixture.processedBatchIds.size, "成功路径应把批置为 PROCESSED")
    }

    @Test
    fun `同 cycle 好坏混投 坏的进死信 好的照常处理`() {
        val fixture = fixture()
        val ack = RecordingAck()
        val dlq = RecordingDlq()
        val bad = record(byteArrayOf(0xFF.toByte(), 0xFF.toByte(), 0xFF.toByte()))
        val good =
            record(
                fixture
                    .message(
                        listOf(PipelineFixture.point(1_000), PipelineFixture.point(2_000, lngQ = PipelineFixture.LNG_Q + 10)),
                    ).toByteArray(),
            )

        listener(fixture, dlq = dlq).onBatches(listOf(bad, good), ack)

        assertEquals(1, ack.count, "整个 cycle 末尾一次性 ack（含被 DLQ 的坏消息）")
        assertEquals("INVALID_PROTOBUF", dlq.published.single().errorCode, "坏消息投 DLQ 留证据")
        assertEquals(1, fixture.processedBatchIds.size, "同 cycle 的好消息不能被坏消息拖累")
        assertEquals(2, fixture.writtenPoints.size)
    }

    @Test
    fun `cycle 内全部坏消息也要 ack 释放分区`() {
        val fixture = fixture()
        val ack = RecordingAck()
        val dlq = RecordingDlq()

        listener(fixture, dlq = dlq).onBatches(listOf(record(byteArrayOf(0xFF.toByte())), record(byteArrayOf(0x00.toByte()))), ack)

        assertEquals(1, ack.count, "没有好消息可处理时同样要 ack，否则分区被两条坏消息永久按住")
        assertEquals(2, dlq.published.size)
        assertTrue(fixture.processedBatchIds.isEmpty())
    }

    @Test
    fun `管道异常向上抛出且不 ack`() {
        val fixture = fixture()
        fixture.tombstoneFailure = IllegalStateException("pg down")
        val ack = RecordingAck()
        val payload = fixture.message(listOf(PipelineFixture.point(1_000))).toByteArray()

        val ex = assertFailsWith<IllegalStateException> { listener(fixture).onBatches(listOf(record(payload)), ack) }

        assertEquals("pg down", ex.message)
        assertEquals(0, ack.count, "可重试异常不能 ack，否则会静默丢批")
    }

    // ------------------------------------------------------------------ 脚手架

    private fun fixture() =
        PipelineFixture().apply {
            // 默认让管道「不该被调用」——成功路径的用例会重新走真件
            tombstoneFailure = null
        }

    private fun listener(
        fixture: PipelineFixture,
        dlq: RecordingDlq = RecordingDlq(),
        status: RecordingStatus = RecordingStatus(),
        registry: SimpleMeterRegistry = SimpleMeterRegistry(),
    ) = TraceBatchListener(fixture.pipeline, dlq, status, ProcessorProperties(), ProcessorMetrics(registry))

    private fun record(value: ByteArray?): ConsumerRecord<String, ByteArray> {
        @Suppress("UNCHECKED_CAST")
        return ConsumerRecord<String, ByteArray?>(TOPIC, PARTITION, OFFSET, PipelineFixture.DEVICE_ID, value)
            as ConsumerRecord<String, ByteArray>
    }

    /** 死信替身：记录 errorCode 与 payload 长度。 */
    private class RecordingDlq : DlqSink {
        val published = mutableListOf<Published>()

        data class Published(
            val key: String?,
            val errorCode: String,
            val batchId: String?,
            val bytes: Int,
        )

        override fun publish(
            key: String?,
            payload: ByteArray,
            errorCode: String,
            errorMessage: String?,
            batchId: String?,
            channel: Int?,
        ) {
            published += Published(key, errorCode, batchId, payload.size)
        }
    }

    /** 台账替身：只记录 DLQ 标记（正常路径的 PROCESSED 由管道内部负责）。 */
    private class RecordingStatus : BatchStatusSink {
        val dlqMarked = mutableListOf<Pair<UUID, String>>()

        override fun markProcessed(batchId: UUID) = Unit

        override fun markFailed(
            batchId: UUID,
            errorCode: String,
        ) = Unit

        override fun markDlq(
            batchId: UUID,
            errorCode: String,
        ) {
            dlqMarked += batchId to errorCode
        }
    }

    private class RecordingAck : Acknowledgment {
        var count: Int = 0

        override fun acknowledge() {
            count++
        }
    }

    private companion object {
        const val TOPIC = "trace.batch.v1"
        const val PARTITION = 0
        const val OFFSET = 0L
    }
}
