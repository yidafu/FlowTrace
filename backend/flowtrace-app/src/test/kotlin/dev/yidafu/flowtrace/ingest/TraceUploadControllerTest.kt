package dev.yidafu.flowtrace.ingest

import dev.yidafu.flowtrace.ingest.config.UploadProperties
import dev.yidafu.flowtrace.ingest.metrics.UploadMetrics
import dev.yidafu.flowtrace.ingest.service.DeviceVersionService
import dev.yidafu.flowtrace.ingest.service.ReteLimiter
import dev.yidafu.flowtrace.ingest.service.TraceBatchWriter
import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.shared.cache.IdempotencyGate
import dev.yidafu.flowtrace.shared.messaging.TraceBatchProducer
import dev.yidafu.flowtrace.shared.model.DeviceStatus
import dev.yidafu.flowtrace.v1.Channel
import dev.yidafu.flowtrace.v1.GeoBase
import dev.yidafu.flowtrace.v1.SdkInfo
import dev.yidafu.flowtrace.v1.TraceBatchHeader
import dev.yidafu.flowtrace.v1.TraceBatchRequest
import dev.yidafu.flowtrace.v1.TracePointDelta
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.BeforeEach
import org.mockito.Mockito
import org.mockito.Mockito.`when`
import org.springframework.http.MediaType
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter
import org.springframework.http.converter.protobuf.ProtobufJsonFormatHttpMessageConverter
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.post
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * 上报控制器测试（standalone MockMvc，注册与生产一致的 Protobuf + Jackson 消息转换器）。
 *
 * 覆盖三类契约：
 * - 两种 Content-Type 都能绑定到 TraceBatchRequest 消息对象（不因格式不支持返回 415）；
 * - P0 回归：Delta 链式解码、repeated 点正确追加、Kafka 分区键为 deviceId；
 * - 响应契约：200 返回 UploadBatchResponse 全字段，失败返回统一错误体（04 §1.4）。
 */
class TraceUploadControllerTest {
    private val deviceVersionService: DeviceVersionService = Mockito.mock(DeviceVersionService::class.java)
    private val producer: TraceBatchProducer = Mockito.mock(TraceBatchProducer::class.java)
    private val limiter: ReteLimiter = Mockito.mock(ReteLimiter::class.java)

    // 台账 writer 用默认应答替身：submit 一律返回 true，避免每个 200 用例都打「队列已满」的 ERROR。
    // 不用 Mockito matcher 打桩——Kotlin 非空参数上 matcher 传 null 会触发 Intrinsics 空指针。
    private val batchWriter: TraceBatchWriter =
        Mockito.mock(TraceBatchWriter::class.java) { invocation ->
            if (invocation.method.name == "submit") true else Mockito.RETURNS_DEFAULTS.answer(invocation)
        }

    // 幂等闸门：默认放行（视为首次上报），单个用例可打桩为 false 验证重复批
    private val idempotencyGate: IdempotencyGate =
        Mockito.mock(IdempotencyGate::class.java) { invocation ->
            if (invocation.method.name == "tryAcquire") true else Mockito.RETURNS_DEFAULTS.answer(invocation)
        }

    /** 内存 registry：断言「哪个分支记了哪个 code」。 */
    private val registry = SimpleMeterRegistry()
    private val metrics = UploadMetrics(registry)

    private val mockMvc: MockMvc =
        MockMvcBuilders
            .standaloneSetup(
                TraceUploadController(
                    deviceVersionService,
                    producer,
                    limiter,
                    UploadProperties(),
                    batchWriter,
                    idempotencyGate,
                    metrics,
                ),
            )
            // Protobuf 转换器负责请求体绑定，Jackson 转换器负责 UploadBatchResponse / 错误体的 JSON 序列化
            .setMessageConverters(ProtobufJsonFormatHttpMessageConverter(), JacksonJsonHttpMessageConverter())
            .build()

    @BeforeEach
    fun setUp() {
        Mockito.reset(deviceVersionService, producer, limiter, idempotencyGate)
        `when`(deviceVersionService.getStatus(Mockito.anyString())).thenReturn(DeviceStatus.ACTIVE)
        `when`(
            deviceVersionService.register(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString(),
            ),
        ).thenReturn(DeviceStatus.ACTIVE)
        `when`(limiter.check("dev-1", Channel.CHANNEL_REALTIME, 3)).thenReturn(true)
    }

    @Test
    fun `application json 请求体按 proto JSON 映射绑定消息对象`() {
        // batchId 是幂等键，入口先校验 UUID（见 422 用例），故 JSON 体必须带合法批头
        val body =
            """{"header":{"batchId":"$validBatchId","schemaVersion":"1.0","deviceId":"dev-1","userId":"u-1",
               "sdk":{"sdkVersion":"1.0.0","platform":"android"}}}"""
        mockMvc
            .post("/v1/traces/batch") {
                contentType = MediaType.APPLICATION_JSON
                content = body
            }.andExpect {
                status { isOk() }
                jsonPath("$.batchId") { value(validBatchId) }
                jsonPath("$.accepted") { value(0) }
                jsonPath("$.deduplicated") { value(false) }
                jsonPath("$.nextUploadIntervalSec") { value(30) }
                jsonPath("$.serverTsMs") { exists() }
            }
        Mockito.verifyNoInteractions(producer)
    }

    @Test
    fun `application x-protobuf 请求体按链式增量解码且以 deviceId 为 kafka 分区键`() {
        val deltas =
            listOf(
                TracePointDelta
                    .newBuilder()
                    .setDTs(0L)
                    .setDLat(0)
                    .setDLng(0)
                    .build(),
                TracePointDelta
                    .newBuilder()
                    .setDTs(100L)
                    .setDLat(10)
                    .setDLng(20)
                    .build(),
                TracePointDelta
                    .newBuilder()
                    .setDTs(50L)
                    .setDLat(-5)
                    .setDLng(30)
                    .build(),
            )

        mockMvc
            .post("/v1/traces/batch") {
                contentType = MediaType.parseMediaType("application/x-protobuf")
                content = request(deltas).toByteArray()
            }.andExpect {
                status { isOk() }
                jsonPath("$.batchId") { value(validBatchId) }
                jsonPath("$.accepted") { value(3) }
                jsonPath("$.nextUploadIntervalSec") { value(30) }
            }

        // 直接读调用记录取实参，避开 Mockito matcher 在 Kotlin 非空参数上返回 null 的问题
        val invocations = Mockito.mockingDetails(producer).invocations
        assertEquals(1, invocations.size)
        val invocation = invocations.single()
        assertEquals("dev-1", invocation.getArgument<String>(0))
        val msg = invocation.getArgument<TraceMessage.TraceBatchMessage>(1)
        assertEquals(3, msg.pointsCount)
        // 基准 (baseTs, 390000000, 1160000000)，逐点累加增量：cur = prev + delta
        assertEquals(baseTs, msg.getPoints(0).tsMs)
        assertEquals(390000000, msg.getPoints(0).latQ)
        assertEquals(baseTs + 100, msg.getPoints(1).tsMs)
        assertEquals(390000010, msg.getPoints(1).latQ)
        assertEquals(1160000020, msg.getPoints(1).lngQ)
        assertEquals(baseTs + 150, msg.getPoints(2).tsMs)
        assertEquals(390000005, msg.getPoints(2).latQ)
        assertEquals(1160000050, msg.getPoints(2).lngQ)
    }

    @Test
    fun `限流时返回 429 与统一错误体`() {
        `when`(limiter.check("dev-1", Channel.CHANNEL_REALTIME, 1)).thenReturn(false)

        mockMvc
            .post("/v1/traces/batch") {
                contentType = MediaType.parseMediaType("application/x-protobuf")
                content = request(listOf(delta(0L))).toByteArray()
            }.andExpect {
                status { isTooManyRequests() }
                header { string("Retry-After", "300") }
                content { contentTypeCompatibleWith(MediaType.APPLICATION_JSON) }
                jsonPath("$.error.code") { value("RATE_LIMITED") }
                jsonPath("$.error.retryAfterSec") { value(300) }
                jsonPath("$.error.details.nextUploadIntervalSec") { value(300) }
            }
        Mockito.verifyNoInteractions(producer)
    }

    @Test
    fun `设备非 ACTIVE 时返回 403 与 DEVICE_DISABLED`() {
        `when`(deviceVersionService.getStatus("dev-1")).thenReturn(DeviceStatus.DISABLED)

        mockMvc
            .post("/v1/traces/batch") {
                contentType = MediaType.parseMediaType("application/x-protobuf")
                content = request(listOf(delta(0L))).toByteArray()
            }.andExpect {
                status { isForbidden() }
                jsonPath("$.error.code") { value("DEVICE_DISABLED") }
            }
        Mockito.verifyNoInteractions(producer)
    }

    @Test
    fun `不支持的 SDK 版本返回 400 与 PROTOCOL_ERROR`() {
        mockMvc
            .post("/v1/traces/batch") {
                contentType = MediaType.parseMediaType("application/x-protobuf")
                content = request(listOf(delta(0L)), sdkVersion = "0.9.0").toByteArray()
            }.andExpect {
                status { isBadRequest() }
                jsonPath("$.error.code") { value("PROTOCOL_ERROR") }
            }
        Mockito.verifyNoInteractions(producer)
    }

    @Test
    fun `batchId 非 UUID 时返回 422 且不写 Kafka、不落台账`() {
        mockMvc
            .post("/v1/traces/batch") {
                contentType = MediaType.parseMediaType("application/x-protobuf")
                content = request(listOf(delta(0L)), batchId = "b-1").toByteArray()
            }.andExpect {
                status { isUnprocessableContent() }
                jsonPath("$.error.code") { value("PROTOCOL_ERROR") }
            }
        Mockito.verifyNoInteractions(producer)
        Mockito.verifyNoInteractions(batchWriter)
    }

    @Test
    fun `重复批返回 deduplicated 且不写 Kafka`() {
        `when`(idempotencyGate.tryAcquire(validBatchId)).thenReturn(false)
        `when`(limiter.check("dev-1", Channel.CHANNEL_REALTIME, 2)).thenReturn(true)

        mockMvc
            .post("/v1/traces/batch") {
                contentType = MediaType.parseMediaType("application/x-protobuf")
                content = request(listOf(delta(0L), delta(10L))).toByteArray()
            }.andExpect {
                status { isOk() }
                jsonPath("$.batchId") { value(validBatchId) }
                jsonPath("$.deduplicated") { value(true) }
                jsonPath("$.accepted") { value(0) }
            }

        Mockito.verifyNoInteractions(producer)
        Mockito.verifyNoInteractions(batchWriter)
    }

    @Test
    fun `Kafka 写入失败时回滚幂等占位再抛错`() {
        `when`(limiter.check("dev-1", Channel.CHANNEL_REALTIME, 1)).thenReturn(true)
        // 默认应答直接抛：模拟 broker 不可用（send().get() 超时/失败）
        val failingProducer = Mockito.mock(TraceBatchProducer::class.java) { throw IllegalStateException("kafka down") }
        val mvc =
            MockMvcBuilders
                .standaloneSetup(
                    TraceUploadController(
                        deviceVersionService,
                        failingProducer,
                        limiter,
                        UploadProperties(),
                        batchWriter,
                        idempotencyGate,
                        metrics,
                    ),
                ).setMessageConverters(ProtobufJsonFormatHttpMessageConverter(), JacksonJsonHttpMessageConverter())
                .build()

        assertFailsWith<Exception> {
            mvc
                .post("/v1/traces/batch") {
                    contentType = MediaType.parseMediaType("application/x-protobuf")
                    content = request(listOf(delta(0L))).toByteArray()
                }
        }

        // D19：占位必须回滚，否则端侧重试会被误判为重复批而丢数据
        Mockito.verify(idempotencyGate).release(validBatchId)
        Mockito.verifyNoInteractions(batchWriter)
    }

    @Test
    fun `请求结果按 code 计数，重复批单独计数`() {
        // 非法 batchId → 422 PROTOCOL_ERROR（走 error() 出口，自动计数）
        mockMvc
            .post("/v1/traces/batch") {
                contentType = MediaType.parseMediaType("application/x-protobuf")
                content = request(listOf(delta(0)), batchId = "not-a-uuid").toByteArray()
            }.andExpect { status { isUnprocessableContent() } }

        // 空批 → 200 OK（走 response() 出口）
        mockMvc
            .post("/v1/traces/batch") {
                contentType = MediaType.parseMediaType("application/x-protobuf")
                content = request(emptyList()).toByteArray()
            }.andExpect { status { isOk() } }

        val protocolErrors = registry.find("ft.upload.requests").tag("code", "PROTOCOL_ERROR").counter()
        val oks = registry.find("ft.upload.requests").tag("code", "OK").counter()
        // 422 必须计入 code 分布，否则 5xx/4xx 占比告警的分母会失真
        assertEquals(1.0, protocolErrors?.count() ?: 0.0)
        // 空批是成功路径，计 OK 而不是被漏掉
        assertEquals(1.0, oks?.count() ?: 0.0)
    }

    @Test
    fun `成功受理的批记录上传耗时，快速拒绝不进直方图`() {
        // 正常批 → 200 且 Kafka 已确认：记一条耗时（3 个点，与 setUp 里 limiter 的打桩一致）
        mockMvc
            .post("/v1/traces/batch") {
                contentType = MediaType.parseMediaType("application/x-protobuf")
                content = request(listOf(delta(0), delta(100), delta(200))).toByteArray()
            }.andExpect { status { isOk() } }

        // 非法 batchId → 422：毫秒级快速拒绝，不许进同一个直方图（否则 p99 被拉低）
        mockMvc
            .post("/v1/traces/batch") {
                contentType = MediaType.parseMediaType("application/x-protobuf")
                content = request(listOf(delta(0)), batchId = "not-a-uuid").toByteArray()
            }.andExpect { status { isUnprocessableContent() } }

        val timer = registry.find("ft.upload.duration").timer()
        assertEquals(1L, timer?.count() ?: 0L)
        // 直方图桶必须导出（publishPercentileHistogram），否则 Prometheus 端 histogram_quantile 算不出 p99
        assertTrue((timer?.takeSnapshot()?.histogramCounts()?.size ?: 0) > 0)
    }

    private fun delta(dTs: Long): TracePointDelta =
        TracePointDelta
            .newBuilder()
            .setDTs(dTs)
            .setDLat(0)
            .setDLng(0)
            .build()

    /** controller 会把 batchId 解析成 UUID（落 trace_batch 主键），测试数据必须是合法 UUID。 */
    private val validBatchId: String = "0190f2a0-0000-7000-8000-000000000001"

    /** 批头基准必须落在实时通道 ts 窗口内，否则字段校验会以 TS_OUT_OF_WINDOW 整批拒收。 */
    private val baseTs: Long = System.currentTimeMillis()

    private fun request(
        deltas: List<TracePointDelta>,
        sdkVersion: String = "1.0.0",
        batchId: String = validBatchId,
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
                    .setChannel(Channel.CHANNEL_REALTIME)
                    .setBase(
                        GeoBase
                            .newBuilder()
                            .setTsMs(baseTs)
                            .setLatQ(390000000)
                            .setLngQ(1160000000),
                    ).setSdk(SdkInfo.newBuilder().setSdkVersion(sdkVersion).setPlatform("android")),
            ).addAllPoints(deltas)
            .build()
}
