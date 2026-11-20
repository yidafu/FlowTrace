package dev.yidafu.flowtrace.shared.web

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.support.DefaultListableBeanFactory
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.test.web.servlet.post
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.test.web.servlet.setup.StandaloneMockMvcBuilder
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import ch.qos.logback.classic.Logger as LogbackLogger

/**
 * 请求日志过滤器契约：
 * - 正常请求只打一条访问日志（不落载荷），且响应体原样透传（copyBodyToResponse 生效）；
 * - 4xx/5xx 额外打印脱敏后的入参/出参；
 * - traceId 头始终回显。
 */
class HttpRequestLoggingFilterTest {
    private val properties = HttpLogProperties()
    private val rendererProvider = DefaultListableBeanFactory().getBeanProvider(RequestPayloadRenderer::class.java)
    private val filter = HttpRequestLoggingFilter(properties, rendererProvider)
    private val mockMvc =
        MockMvcBuilders
            .standaloneSetup(FakeController())
            // addFilters 返回泛型自类型，Kotlin 需显式指定，否则链式 build() 不可解析
            .addFilters<StandaloneMockMvcBuilder>(TraceIdFilter(), filter)
            .build()
    private val appender = ListAppender<ILoggingEvent>()
    private val filterLogger = LoggerFactory.getLogger(HttpRequestLoggingFilter::class.java) as LogbackLogger

    @BeforeEach
    fun setUp() {
        appender.start()
        filterLogger.addAppender(appender)
    }

    @AfterEach
    fun tearDown() {
        filterLogger.detachAppender(appender)
        appender.list.clear()
    }

    @Test
    fun `正常请求只打访问日志且响应体透传给客户端`() {
        mockMvc
            .post("/ok") {
                contentType = MediaType.APPLICATION_JSON
                content = """{"latQ":390000000}"""
            }.andExpect {
                status { isOk() }
                header { exists(TraceContext.TRACE_ID_HEADER) }
                jsonPath("$.latQ") { value(390000000) }
            }

        val messages = appender.list.map { it.formattedMessage }
        assertTrue(messages.any { it.startsWith("http 请求完成") }, "缺少访问日志：$messages")
        assertFalse(messages.any { it.contains("入参=") }, "成功请求不应输出载荷：$messages")
    }

    @Test
    fun `4xx 时输出脱敏后的入参与出参`() {
        mockMvc
            .post("/reject") {
                contentType = MediaType.APPLICATION_JSON
                content = """{"latQ":390000000,"lngQ":1160000000,"points":3}"""
            }.andExpect {
                status { isBadRequest() }
                content { string("""{"error":"boom"}""") }
            }

        val event = appender.list.single { it.formattedMessage.contains("http 请求失败") }
        assertEquals(Level.WARN, event.level)
        val message = event.formattedMessage
        assertContains(message, "入参=")
        assertContains(message, "出参=")
        assertContains(message, "\"latQ\":\"***\"")
        assertContains(message, "\"points\":3")
        assertFalse(message.contains("390000000"), "日志不得输出坐标明文：$message")
    }

    @Test
    fun `5xx 记 ERROR 且可按开关关闭载荷`() {
        properties.payloadOnError = false

        mockMvc
            .post("/broken") {
                contentType = MediaType.APPLICATION_JSON
                content = """{"latQ":390000000}"""
            }.andExpect { status { isInternalServerError() } }

        val event = appender.list.single { it.formattedMessage.contains("http 请求失败") }
        assertEquals(Level.ERROR, event.level)
        assertContains(event.formattedMessage, "payload-on-error=false")
        assertFalse(event.formattedMessage.contains("390000000"))
    }

    /** 最小控制器：只验证过滤器行为，不引入业务依赖。 */
    @RestController
    class FakeController {
        @PostMapping("/ok")
        fun ok(
            @RequestBody body: Map<String, Any?>,
        ): Map<String, Any?> = body

        @PostMapping("/reject")
        fun reject(
            @RequestBody body: Map<String, Any?>,
        ): ResponseEntity<Map<String, String>> = ResponseEntity.badRequest().body(mapOf("error" to "boom"))

        @PostMapping("/broken")
        fun broken(
            @RequestBody body: Map<String, Any?>,
        ): ResponseEntity<Map<String, String>> = ResponseEntity.status(500).body(mapOf("error" to "boom"))
    }
}
