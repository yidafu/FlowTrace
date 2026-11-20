package dev.yidafu.flowtrace.ingest.web

import jakarta.servlet.FilterChain
import jakarta.servlet.ReadListener
import jakarta.servlet.ServletInputStream
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletRequestWrapper
import jakarta.servlet.http.HttpServletResponse
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import java.io.InputStream
import java.util.zip.GZIPInputStream

/**
 * 请求体 gzip 透明解压过滤器（Content-Encoding: gzip）。
 *
 * 上报主格式为 Protobuf + GZIP（04 §2）：解压发生在过滤器层，消息转换器按
 * Content-Type 绑定消息对象时拿到的已是解压后的字节流。
 *
 * TODO: 按 04 §2 强制校验——POST /v1/traces/batch 缺失 Content-Encoding: gzip
 * 时按 INVALID_GZIP 拒绝（当前放行明文请求）。
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
class GzipRequestDecompressFilter : OncePerRequestFilter() {
    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain,
    ) {
        if ("gzip".equals(request.getHeader(CONTENT_ENCODING), ignoreCase = true)) {
            filterChain.doFilter(GzipHttpServletRequestWrapper(request), response)
        } else {
            filterChain.doFilter(request, response)
        }
    }

    /** 解压包装：流读取经 GZIPInputStream；解压后长度未知，Content-Length 置 -1（分块读取）。 */
    private class GzipHttpServletRequestWrapper(
        request: HttpServletRequest,
    ) : HttpServletRequestWrapper(request) {
        // 只在这里包一层：GzipServletInputStream 内部已经套了 GZIPInputStream，
        // 再包一层会对「已解压的明文」做第二次解压，直接抛 ZipException（Not in GZIP format）
        override fun getInputStream(): ServletInputStream = GzipServletInputStream(super.getInputStream())

        override fun getContentLengthLong(): Long = -1L

        @Deprecated("Deprecated in Java")
        override fun getContentLength(): Int = -1
    }

    /**
     * 包在 GZIPInputStream 上的 Servlet 输入流。
     *
     * 只实现同步读取所需的最小面：`isFinished` 在读到 EOF 时置位（容器判断请求体读尽），
     * `setReadListener` 直接抛——本链路是同步 MVC，没有异步 body 读写路径。
     */
    private class GzipServletInputStream(
        input: InputStream,
    ) : ServletInputStream() {
        private val gzip = GZIPInputStream(input)
        private var finished = false

        override fun read(): Int {
            val b = gzip.read()
            if (b == -1) {
                finished = true
            }
            return b
        }

        override fun available(): Int = gzip.available()

        override fun isFinished(): Boolean = finished

        override fun isReady(): Boolean = true

        override fun setReadListener(readListener: ReadListener?) = throw UnsupportedOperationException("非异步请求不支持 ReadListener")
    }

    private companion object {
        const val CONTENT_ENCODING = "Content-Encoding"
    }
}
