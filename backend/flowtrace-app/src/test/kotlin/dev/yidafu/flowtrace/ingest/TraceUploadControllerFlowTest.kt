package dev.yidafu.flowtrace.ingest

import dev.yidafu.flowtrace.ingest.config.UploadProperties
import dev.yidafu.flowtrace.ingest.dto.UploadBatchResponse
import dev.yidafu.flowtrace.ingest.service.DeviceVersionService
import dev.yidafu.flowtrace.ingest.service.ReteLimiter
import dev.yidafu.flowtrace.ingest.service.TraceBatchWriter
import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.shared.cache.FlowtraceCache
import dev.yidafu.flowtrace.shared.cache.IdempotencyGate
import dev.yidafu.flowtrace.shared.messaging.TraceBatchProducer
import dev.yidafu.flowtrace.shared.model.Channel
import dev.yidafu.flowtrace.shared.model.DeviceStatus
import dev.yidafu.flowtrace.shared.model.TraceBatchStatus
import dev.yidafu.flowtrace.shared.persistence.repository.DeviceRepository
import dev.yidafu.flowtrace.shared.persistence.repository.TraceBatchRepository
import dev.yidafu.flowtrace.shared.persistence.repository.TraceBatchRow
import dev.yidafu.flowtrace.shared.web.ApiErrorResponse
import dev.yidafu.flowtrace.v1.GeoBase
import dev.yidafu.flowtrace.v1.LocationSource
import dev.yidafu.flowtrace.v1.SdkInfo
import dev.yidafu.flowtrace.v1.TraceBatchHeader
import dev.yidafu.flowtrace.v1.TraceBatchRequest
import dev.yidafu.flowtrace.v1.TracePointDelta
import kotlinx.coroutines.Dispatchers
import org.mockito.Mockito
import org.mockito.stubbing.Answer
import org.springframework.http.HttpStatus
import java.time.Duration
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertTrue
import dev.yidafu.flowtrace.v1.Channel as ProtoChannel

/**
 * 上报处理链的行为测试（04 §2 / 06 §2.2）：直接调控制器，不走 MVC。
 *
 * 与 `TraceUploadControllerTest`（编码/响应契约）互补，这里盯的是**副作用与顺序**：
 * - 空批是「端侧心跳」，必须短路：不注册设备、不占限流额度、不写 Kafka、不落台账；
 * - `trace_batch` 行的每个字段都来自批头（first/last ts 不能取错成批内首末增量）；
 * - 台账队列满只能丢台账，**不能反压主链路**（Kafka 已确认，响应必须仍为 200）；
 * - Kafka 写失败必须抛出去（端侧按退避重试），且此时不能落「已接收」台账；
 * - batchId 校验必须在设备状态之前——非法批不该触发任何回源查询。
 */
class TraceUploadControllerFlowTest {
    private val deviceService = RecordingDeviceService()
    private val limiter = RecordingLimiter()
    private val writer = RecordingWriter()
    private val gate = RecordingGate()

    private val producer: TraceBatchProducer = Mockito.mock(TraceBatchProducer::class.java)

    private fun controller(props: UploadProperties = UploadProperties()) =
        TraceUploadController(deviceService, producer, limiter, props, writer, gate)

    /** 幂等闸门替身：记录占用/回滚，`duplicate=true` 时模拟重复批。 */
    private class RecordingGate : IdempotencyGate {
        val acquired = mutableListOf<String>()
        val released = mutableListOf<String>()
        var duplicate: Boolean = false

        override fun tryAcquire(key: String): Boolean {
            acquired += key
            return !duplicate
        }

        override fun release(key: String) {
            released += key
        }
    }

    @Test
    fun `空批返回 200 但不注册设备 不占限流 不写 Kafka 不落台账`() {
        val response = controller().uploadBatch(request(emptyList()))

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(0, (response.body as UploadBatchResponse).accepted)
        assertEquals(0, deviceService.registerCalls, "空批不该触发设备注册")
        assertEquals(1, deviceService.getStatusCalls, "入口仍需做只读状态检查")
        assertEquals(emptyList(), limiter.calls, "空批不占限流额度")
        assertTrue(producerInvocations().isEmpty(), "空批不该写 Kafka")
        assertTrue(writer.rows.isEmpty(), "空批不该落台账")
    }

    @Test
    fun `台账字段全部取自批头与批内首末点`() {
        // Delta 是增量：累计 0 / 1000 / 1500，末点绝对时间 = base + 1500
        val points = listOf(delta(0, dLat = 0), delta(1_000, dLat = 10), delta(500, dLat = 20))

        controller().uploadBatch(request(points, clientTsMs = CLIENT_TS_MS))

        val row = writer.rows.single()
        assertEquals(UUID.fromString(BATCH_ID), row.batchId)
        assertEquals("u-1", row.userId)
        assertEquals("dev-1", row.deviceId)
        assertEquals(Channel.REALTIME, row.channel)
        assertEquals(3, row.pointCount)
        // first_ts 取批头基准（= 批内首点绝对值），last_ts 取 Delta 链展开后的末点
        assertEquals(BASE_TS_MS, row.firstTs.toEpochMilli())
        assertEquals(BASE_TS_MS + 1_500, row.lastTs.toEpochMilli())
        assertEquals(CLIENT_TS_MS, row.clientTs.toEpochMilli())
        assertEquals("1.0.0", row.sdkVersion)
        assertEquals("android", row.platform)
        assertEquals(TraceBatchStatus.ACCEPTED, row.status, "upload 只写 ACCEPTED，PROCESSED 由 processor 负责")
    }

    @Test
    fun `补传通道按 backfill 落台账且走补传限流档`() {
        val response =
            controller().uploadBatch(
                request(listOf(delta(0)), channel = ProtoChannel.CHANNEL_BACKFILL),
            )

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(Channel.BACKFILL, writer.rows.single().channel)
        assertEquals(ProtoChannel.CHANNEL_BACKFILL, limiter.calls.single().channel)
    }

    @Test
    fun `台账队列满只丢台账 上报仍返回 200`() {
        writer.accept = false

        val response = controller().uploadBatch(request(listOf(delta(0), delta(1_000))))

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals(2, (response.body as UploadBatchResponse).accepted, "Kafka 已确认，点数照常回给端侧")
        assertTrue(writer.rows.isEmpty())
    }

    @Test
    fun `Kafka 写入失败向上抛出且不落台账`() {
        val failing = Mockito.mock(TraceBatchProducer::class.java, Answer<Any> { throw RuntimeException("kafka down") })

        val ex =
            assertFails {
                TraceUploadController(deviceService, failing, limiter, UploadProperties(), writer, gate)
                    .uploadBatch(request(listOf(delta(0))))
            }

        assertTrue(errorChain(ex).any { it.message?.contains("kafka down") == true }, "原始异常不能被吞：${errorChain(ex)}")
        assertTrue(writer.rows.isEmpty(), "Kafka 未确认就不能写『已接收』台账")
        assertEquals(listOf(BATCH_ID), gate.released, "Kafka 失败必须回滚幂等占位，否则端侧重试会被当成重复批（D19）")
    }

    @Test
    fun `重复批不写 Kafka 但仍返回 200`() {
        gate.duplicate = true

        val response = controller().uploadBatch(request(listOf(delta(0), delta(1_000))))

        assertEquals(HttpStatus.OK, response.statusCode)
        val body = response.body as UploadBatchResponse
        assertTrue(body.deduplicated, "命中幂等要明确告诉 SDK 这是重复批")
        assertEquals(0, body.accepted, "未写 Kafka 就不该回报点数")
        assertTrue(producerInvocations().isEmpty(), "重复批不能重复写 Kafka")
        assertTrue(writer.rows.isEmpty())
        assertEquals(listOf(BATCH_ID), gate.acquired)
    }

    @Test
    fun `ACCEPTED 台账不留 processed_at`() {
        controller().uploadBatch(request(listOf(delta(0))))

        val row = writer.rows.single()
        assertEquals(null, row.processedAt, "ACCEPTED 阶段还没处理，processed_at 必须为空")
        assertEquals(null, row.errorCode)
    }

    @Test
    fun `batchId 非法时先于设备状态校验失败且不回源`() {
        deviceService.status = DeviceStatus.DISABLED

        val response = controller().uploadBatch(request(listOf(delta(0)), batchId = "b-1"))

        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, response.statusCode)
        assertEquals("PROTOCOL_ERROR", (response.body as ApiErrorResponse).error.code)
        assertEquals(0, deviceService.getStatusCalls, "非法 batchId 不该触发状态回源")
        assertEquals(0, deviceService.registerCalls)
        assertEquals(emptyList(), limiter.calls)
    }

    @Test
    fun `设备非 ACTIVE 时在写 Kafka 之前拒绝`() {
        deviceService.status = DeviceStatus.SUSPENDED

        val response = controller().uploadBatch(request(listOf(delta(0))))

        assertEquals(HttpStatus.FORBIDDEN, response.statusCode)
        assertEquals("DEVICE_DISABLED", (response.body as ApiErrorResponse).error.code)
        assertTrue(producerInvocations().isEmpty())
        assertEquals(emptyList(), limiter.calls, "拒收请求不该占限流额度")
        assertTrue(writer.rows.isEmpty())
    }

    @Test
    fun `点级非增量字段逐点透传 且 idx 为批内序号`() {
        val first =
            TracePointDelta
                .newBuilder()
                .setDTs(0)
                .setAccuracyCm(500)
                .setSpeedMps(1.5f)
                .setBearingDeg(90)
                .setAltitudeCm(1_200)
                .setSource(LocationSource.SOURCE_GPS)
                .setFlags(0x01)
                .build()
        val second =
            TracePointDelta
                .newBuilder()
                .setDTs(1_000)
                .setAccuracyCm(0)
                .setSpeedMps(0f)
                .build()

        controller().uploadBatch(request(listOf(first, second)))

        val message = sentMessage()
        val p0 = message.getPoints(0)
        assertEquals(500, p0.accuracyCm)
        assertEquals(1.5f, p0.speedMps)
        assertEquals(90, p0.bearingDeg)
        assertEquals(1_200, p0.altitudeCm)
        assertEquals(1, p0.source)
        assertEquals(0x01, p0.flags)
        assertEquals(0, p0.idx)

        val p1 = message.getPoints(1)
        assertEquals(0, p1.accuracyCm, "缺省字段原样透传，收敛为 null 是 processor 的事")
        assertEquals(0f, p1.speedMps)
        assertEquals(BASE_TS_MS + 1_000, p1.tsMs, "Delta 链式累加的绝对值必须落在批头基准上")
        assertEquals(1, p1.idx)
    }

    @Test
    fun `SDK 版本只接受 1_0_x`() {
        listOf("1.0", "1.0.0", "1.0.7").forEach { version ->
            assertEquals(
                HttpStatus.OK,
                controller().uploadBatch(request(listOf(delta(0)), sdkVersion = version)).statusCode,
                "$version 应放行",
            )
        }
        listOf("1.1.0", "0.9.0", "").forEach { version ->
            val response = controller().uploadBatch(request(listOf(delta(0)), sdkVersion = version))
            assertEquals(HttpStatus.BAD_REQUEST, response.statusCode, "$version 应拒绝")
            assertEquals("PROTOCOL_ERROR", (response.body as ApiErrorResponse).error.code)
        }
    }

    @Test
    fun `下发间隔被钳制到 10 至 300 秒`() {
        val tooSmall = controller(UploadProperties().apply { intervalBaseSec = 5 }).uploadBatch(request(emptyList()))
        assertEquals(10, (tooSmall.body as UploadBatchResponse).nextUploadIntervalSec)

        val tooLarge = controller(UploadProperties().apply { intervalBaseSec = 1_000 }).uploadBatch(request(emptyList()))
        assertEquals(300, (tooLarge.body as UploadBatchResponse).nextUploadIntervalSec)
    }

    @Test
    fun `限流响应的 Retry-After 同样被钳制`() {
        limiter.allow = false

        val response =
            controller(UploadProperties().apply { intervalLimitedSec = 1_000 })
                .uploadBatch(request(listOf(delta(0))))

        assertEquals(HttpStatus.TOO_MANY_REQUESTS, response.statusCode)
        assertEquals("300", response.headers.getFirst("Retry-After"))
        assertEquals(300L, (response.body as ApiErrorResponse).error.retryAfterSec)
    }

    // ------------------------------------------------------------------ 测试脚手架

    private fun producerInvocations() = Mockito.mockingDetails(producer).invocations.toList()

    private fun sentMessage(): TraceMessage.TraceBatchMessage {
        val invocation = producerInvocations().single()
        assertEquals("dev-1", invocation.getArgument<String>(0), "Kafka 分区键必须是 deviceId")
        return invocation.getArgument(1)
    }

    private fun errorChain(error: Throwable): List<Throwable> {
        val chain = mutableListOf<Throwable>()
        var current: Throwable? = error
        while (current != null && chain.none { it === current }) {
            chain += current
            current = current.cause
        }
        return chain
    }

    private fun delta(
        dTs: Long,
        dLat: Int = 0,
        dLng: Int = 0,
    ): TracePointDelta =
        TracePointDelta
            .newBuilder()
            .setDTs(dTs)
            .setDLat(dLat.toLong())
            .setDLng(dLng.toLong())
            .build()

    private fun request(
        points: List<TracePointDelta>,
        batchId: String = BATCH_ID,
        sdkVersion: String = "1.0.0",
        channel: ProtoChannel = ProtoChannel.CHANNEL_REALTIME,
        clientTsMs: Long = BASE_TS_MS,
    ): TraceBatchRequest =
        TraceBatchRequest
            .newBuilder()
            .setHeader(
                TraceBatchHeader
                    .newBuilder()
                    .setBatchId(batchId)
                    .setSchemaVersion("1.0")
                    .setDeviceId("dev-1")
                    .setUserId("u-1")
                    .setSessionId("s-1")
                    .setTaskId("t-1")
                    .setChannel(channel)
                    .setClientTsMs(clientTsMs)
                    .setBase(
                        GeoBase
                            .newBuilder()
                            .setTsMs(BASE_TS_MS)
                            .setLatQ(390000000)
                            .setLngQ(1160000000),
                    ).setSdk(SdkInfo.newBuilder().setSdkVersion(sdkVersion).setPlatform("android")),
            ).addAllPoints(points)
            .build()

    /** 设备服务替身：只记录调用，状态由用例直接给定。 */
    private class RecordingDeviceService : DeviceVersionService(DeviceRepository()) {
        var status: DeviceStatus = DeviceStatus.ACTIVE
        var getStatusCalls = 0
        var registerCalls = 0

        override fun getStatus(deviceId: String): DeviceStatus {
            getStatusCalls++
            return status
        }

        override fun register(
            deviceId: String,
            userId: String,
            platform: String,
            appVersion: String,
            sdkVersion: String,
        ): DeviceStatus {
            registerCalls++
            return status
        }
    }

    /** 限流器替身：`allow` 控制放行，`calls` 用于断言「是否消耗额度」。 */
    private class RecordingLimiter : ReteLimiter(InMemoryCache(), UploadProperties()) {
        var allow: Boolean = true
        val calls = mutableListOf<Call>()

        data class Call(
            val deviceId: String,
            val channel: ProtoChannel,
            val pointCount: Int,
        )

        override fun check(
            deviceId: String,
            channel: ProtoChannel,
            pointCount: Int,
        ): Boolean {
            calls += Call(deviceId, channel, pointCount)
            return allow
        }
    }

    /** 台账写入替身：`accept=false` 模拟队列满/停机（`submit` 返回 false）。 */
    private class RecordingWriter : TraceBatchWriter(TraceBatchRepository(), Dispatchers.Unconfined) {
        val rows = mutableListOf<TraceBatchRow>()
        var accept: Boolean = true

        override fun submit(row: TraceBatchRow): Boolean {
            if (!accept) return false
            rows += row
            return true
        }
    }

    private class InMemoryCache : FlowtraceCache {
        private val values = mutableMapOf<String, Any>()

        override fun has(key: String): Boolean = values.containsKey(key)

        override fun <T : Any> get(
            key: String,
            type: Class<T>,
        ): T? = values[key]?.let { type.cast(it) }

        override fun put(
            key: String,
            value: Any,
            ttl: Duration?,
        ) {
            values[key] = value
        }

        override fun evict(key: String) {
            values.remove(key)
        }

        override fun incrementBy(
            key: String,
            delta: Long,
            ttl: Duration?,
        ): Long = throw UnsupportedOperationException("本替身不实现计数")
    }

    private companion object {
        const val BATCH_ID = "0190f2a0-0000-7000-8000-000000000002"

        /**
         * 批头基准时间必须落在实时通道的 ts 窗口内（now−48h ~ now+10min），
         * 否则字段校验会以 `TS_OUT_OF_WINDOW` 整批拒收——所以用运行时刻而不是写死的时间戳。
         */
        val BASE_TS_MS = System.currentTimeMillis()
        val CLIENT_TS_MS = BASE_TS_MS + 500
    }
}
