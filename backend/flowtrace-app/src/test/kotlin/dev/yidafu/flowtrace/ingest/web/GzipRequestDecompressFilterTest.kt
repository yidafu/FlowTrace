package dev.yidafu.flowtrace.ingest.web

import jakarta.servlet.FilterChain
import jakarta.servlet.ServletInputStream
import jakarta.servlet.http.HttpServletRequest
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.mock.web.MockHttpServletResponse
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * 请求体 gzip 透明解压测试（04 §2：上报主格式 Protobuf + GZIP）。
 *
 * 过滤器在最高优先级执行，消息转换器拿到的是解压后的字节流；解压后长度未知，
 * Content-Length 必须收敛为 -1（否则容器/转换器会按压缩前的长度读，尾部截断）。
 * 非 gzip 请求必须原样透传——包括调试用的 `application/json` 明文上报。
 */
class GzipRequestDecompressFilterTest {
    private val filter = GzipRequestDecompressFilter()

    @Test
    fun `Content-Encoding gzip 时请求体被透明解压`() {
        val plain = PLAIN_BODY.toByteArray()
        val request =
            MockHttpServletRequest("POST", PATH).apply {
                addHeader("Content-Encoding", "gzip")
                contentType = "application/x-protobuf"
                setContent(gzip(plain))
            }

        val captured = capture(request)

        assertContentEquals(plain, captured.body, "解压后的请求体应与原始明文一致")
        assertEquals(-1L, captured.contentLengthLong, "解压后长度未知，Content-Length 必须为 -1")
        assertEquals(-1, captured.contentLength, "getContentLength 同样要返回 -1")
        assertTrue(captured.finished, "读完整个流后 isFinished 应为 true")
    }

    @Test
    fun `Content-Encoding 判定大小写不敏感`() {
        val plain = "abc".toByteArray()
        val request =
            MockHttpServletRequest("POST", PATH).apply {
                addHeader("Content-Encoding", "GZip")
                setContent(gzip(plain))
            }

        assertContentEquals(plain, capture(request).body)
    }

    @Test
    fun `未声明 gzip 的请求原样透传`() {
        val plain = PLAIN_BODY.toByteArray()
        val request =
            MockHttpServletRequest("POST", PATH).apply {
                contentType = "application/json"
                setContent(plain)
            }

        val captured = capture(request)

        assertContentEquals(plain, captured.body)
        assertEquals(plain.size.toLong(), captured.contentLengthLong, "非 gzip 请求不该改写 Content-Length")
    }

    @Test
    fun `gzip 请求体的原始压缩字节不会漏给下游`() {
        val plain = PLAIN_BODY.toByteArray()
        val compressed = gzip(plain)
        val request =
            MockHttpServletRequest("POST", PATH).apply {
                addHeader("Content-Encoding", "gzip")
                setContent(compressed)
            }

        val captured = capture(request)

        assertContentEquals(plain, captured.body)
        assertFalse(captured.body.contentEquals(compressed), "下游拿到的不能是压缩字节")
    }

    private fun capture(request: MockHttpServletRequest): Captured {
        val captured = Captured()
        filter.doFilter(
            request,
            MockHttpServletResponse(),
            FilterChain { req, _ ->
                val httpRequest = req as HttpServletRequest
                captured.contentLengthLong = httpRequest.contentLengthLong
                captured.contentLength = httpRequest.contentLength
                val stream: ServletInputStream = httpRequest.inputStream
                captured.body = stream.readBytes()
                captured.finished = stream.isFinished
            },
        )
        return captured
    }

    private fun gzip(plain: ByteArray): ByteArray {
        val out = ByteArrayOutputStream()
        GZIPOutputStream(out).use { it.write(plain) }
        return out.toByteArray()
    }

    private class Captured {
        var body: ByteArray = ByteArray(0)
        var contentLengthLong: Long = 0
        var contentLength: Int = 0
        var finished: Boolean = false
    }

    private companion object {
        const val PATH = "/v1/traces/batch"
        const val PLAIN_BODY = """{"header":{"batchId":"0190f2a0-0000-7000-8000-000000000001"}}"""
    }
}
