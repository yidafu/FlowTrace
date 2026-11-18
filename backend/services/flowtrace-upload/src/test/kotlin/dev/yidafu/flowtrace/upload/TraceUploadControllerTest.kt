package dev.yidafu.flowtrace.upload

import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.messaging.TraceBatchProducer
import dev.yidafu.flowtrace.model.DeviceStatus
import dev.yidafu.flowtrace.upload.config.UploadProperties
import dev.yidafu.flowtrace.upload.service.DeviceVersionService
import dev.yidafu.flowtrace.upload.service.ReteLimiter
import dev.yidafu.flowtrace.v1.Channel
import dev.yidafu.flowtrace.v1.GeoBase
import dev.yidafu.flowtrace.v1.SdkInfo
import dev.yidafu.flowtrace.v1.TraceBatchHeader
import dev.yidafu.flowtrace.v1.TraceBatchRequest
import dev.yidafu.flowtrace.v1.TracePointDelta
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

    private val mockMvc: MockMvc =
        MockMvcBuilders
            .standaloneSetup(TraceUploadController(deviceVersionService, producer, limiter, UploadProperties()))
            // Protobuf 转换器负责请求体绑定，Jackson 转换器负责 UploadBatchResponse / 错误体的 JSON 序列化
            .setMessageConverters(ProtobufJsonFormatHttpMessageConverter(), JacksonJsonHttpMessageConverter())
            .build()

    @BeforeEach
    fun setUp() {
        Mockito.reset(deviceVersionService, producer, limiter)
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
        mockMvc
            .post("/v1/traces/batch") {
                contentType = MediaType.APPLICATION_JSON
                content = "{}"
            }.andExpect {
                status { isOk() }
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
                jsonPath("$.batchId") { value("b-1") }
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
        // 基准 (1000, 390000000, 1160000000)，逐点累加增量：cur = prev + delta
        assertEquals(1000L, msg.getPoints(0).tsMs)
        assertEquals(390000000, msg.getPoints(0).latQ)
        assertEquals(1100L, msg.getPoints(1).tsMs)
        assertEquals(390000010, msg.getPoints(1).latQ)
        assertEquals(1160000020, msg.getPoints(1).lngQ)
        assertEquals(1150L, msg.getPoints(2).tsMs)
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

    private fun delta(dTs: Long): TracePointDelta =
        TracePointDelta
            .newBuilder()
            .setDTs(dTs)
            .setDLat(0)
            .setDLng(0)
            .build()

    private fun request(
        deltas: List<TracePointDelta>,
        sdkVersion: String = "1.0.0",
    ): TraceBatchRequest =
        TraceBatchRequest
            .newBuilder()
            .setHeader(
                TraceBatchHeader
                    .newBuilder()
                    .setBatchId("b-1")
                    .setSchemaVersion("1.0")
                    .setDeviceId("dev-1")
                    .setUserId("u-1")
                    .setChannel(Channel.CHANNEL_REALTIME)
                    .setBase(
                        GeoBase
                            .newBuilder()
                            .setTsMs(1000L)
                            .setLatQ(390000000)
                            .setLngQ(1160000000),
                    ).setSdk(SdkInfo.newBuilder().setSdkVersion(sdkVersion).setPlatform("android")),
            ).addAllPoints(deltas)
            .build()
}
