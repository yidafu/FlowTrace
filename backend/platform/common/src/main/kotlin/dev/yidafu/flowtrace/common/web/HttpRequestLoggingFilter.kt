package dev.yidafu.flowtrace.common.web

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import org.springframework.web.util.ContentCachingRequestWrapper
import org.springframework.web.util.ContentCachingResponseWrapper
import java.nio.charset.StandardCharsets

/**
 * 请求日志过滤器：每个请求一行访问日志，异常请求额外带「入参 + 出参」。
 *
 * 分层职责（与 [GlobalExceptionHandler] 配合）：
 * - 本过滤器负责**载荷**：请求体、响应体、耗时、状态码；
 * - 异常处理器负责**语义**：错误码、根因与堆栈。
 * 两者共享同一 traceId（[TraceIdFilter] 写入），按 traceId 关联即可。
 *
 * 顺序：`HIGHEST_PRECEDENCE + 20`，即排在 gzip 解压过滤器之后——缓存下来的是**解压后**字节，
 * 否则打出来是二进制乱码。上游 gzip 过滤器仍是最外层。
 *
 * 隐私：任何落日志的载荷都要过 [PayloadMasking]（07 §5 日志不出坐标）。
 * 流式/下载类响应（`Accept: octet-stream`、`text/event-stream` 等）只记状态与耗时，不缓存 body。
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 20)
class HttpRequestLoggingFilter(
    private val properties: HttpLogProperties,
    private val payloadRenderers: ObjectProvider<RequestPayloadRenderer>,
) : OncePerRequestFilter() {
    private val log = LoggerFactory.getLogger(HttpRequestLoggingFilter::class.java)

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain,
    ) {
        if (!properties.requestEnabled) {
            filterChain.doFilter(request, response)
            return
        }
        val cachedRequest = ContentCachingRequestWrapper(request, properties.maxPayloadChars)
        // 下载/SSE 这类大载荷或流式响应不缓存 body：ContentCachingResponseWrapper 会把整份内容留在内存里
        val cachedResponse = if (isStreaming(request)) null else ContentCachingResponseWrapper(response)
        val startNs = System.nanoTime()
        var failure: Throwable? = null
        try {
            filterChain.doFilter(cachedRequest, cachedResponse ?: response)
        } catch (ex: Throwable) {
            failure = ex
            throw ex
        } finally {
            val costMs = (System.nanoTime() - startNs) / NANOS_PER_MILLI
            runCatching {
                writeAccessLog(cachedRequest, cachedResponse ?: response, costMs, failure)
            }.onFailure { log.warn("请求日志输出失败，已跳过：traceId={}", TraceContext.currentTraceId(), it) }
            // 响应体已被缓存包装器拦下，必须写回真实响应，否则客户端拿不到 body
            cachedResponse?.let { runCatching { it.copyBodyToResponse() } }
        }
    }

    private fun writeAccessLog(
        request: ContentCachingRequestWrapper,
        response: HttpServletResponse,
        costMs: Long,
        failure: Throwable?,
    ) {
        val status = response.status
        val serverError = failure != null || status >= HTTP_SERVER_ERROR
        val clientError = !serverError && status >= HTTP_BAD_REQUEST
        val traceId = TraceContext.currentTraceId()

        if (!serverError && !clientError) {
            log.info(
                "http 请求完成 traceId={} method={} uri={} query={} status={} costMs={} deviceId={} userId={}",
                traceId,
                request.method,
                request.requestURI,
                request.queryString ?: "-",
                status,
                costMs,
                request.getHeader(HEADER_DEVICE_ID) ?: "-",
                request.getHeader(HEADER_USER_ID) ?: "-",
            )
            return
        }

        val payload =
            if (properties.payloadOnError) {
                buildString {
                    append("入参=").append(describeRequestBody(request))
                    append(", 出参=").append(describeResponseBody(response))
                }
            } else {
                "入参/出参已关闭（flowtrace.log.payload-on-error=false）"
            }
        val message = "http 请求失败 traceId={} method={} uri={} query={} status={} costMs={} deviceId={} {} "
        if (serverError) {
            log.error(
                message,
                traceId,
                request.method,
                request.requestURI,
                request.queryString ?: "-",
                status,
                costMs,
                request.getHeader(HEADER_DEVICE_ID) ?: "-",
                payload,
                failure,
            )
        } else {
            log.warn(
                message,
                traceId,
                request.method,
                request.requestURI,
                request.queryString ?: "-",
                status,
                costMs,
                request.getHeader(HEADER_DEVICE_ID) ?: "-",
                payload,
            )
        }
    }

    private fun describeRequestBody(request: ContentCachingRequestWrapper): String {
        val contentType = request.contentType
        val body = request.contentAsByteArray
        if (body.isEmpty()) return "<空>"
        return describePayload(contentType, body)
    }

    private fun describeResponseBody(response: HttpServletResponse): String {
        if (response !is ContentCachingResponseWrapper) return "<未缓存：流式/大载荷响应>"
        val body = response.contentAsByteArray
        if (body.isEmpty()) return "<空>"
        return describePayload(response.contentType, body)
    }

    private fun describePayload(
        contentType: String?,
        body: ByteArray,
    ): String {
        val renderer =
            payloadRenderers
                .orderedStream()
                .filter { it.supports(contentType) }
                .findFirst()
                .orElse(null)
        val text =
            when {
                renderer != null -> renderer.render(contentType, body)
                isTextual(contentType) -> PayloadMasking.mask(body.toString(StandardCharsets.UTF_8))
                else -> "<二进制 ${body.size}B>"
            }
        return PayloadMasking.truncate(text, properties.maxPayloadChars)
    }

    private fun isTextual(contentType: String?): Boolean {
        val normalized = contentType?.lowercase() ?: return false
        return TEXTUAL_MARKERS.any { normalized.contains(it) }
    }

    /** 客户端要求下载/流式响应时只记状态与耗时，不缓存 body。 */
    private fun isStreaming(request: HttpServletRequest): Boolean {
        val accept = request.getHeader(HEADER_ACCEPT)?.lowercase() ?: return false
        return STREAMING_MARKERS.any { accept.contains(it) }
    }

    private companion object {
        const val NANOS_PER_MILLI = 1_000_000L
        const val HTTP_BAD_REQUEST = 400
        const val HTTP_SERVER_ERROR = 500
        const val HEADER_DEVICE_ID = "X-Device-Id"
        const val HEADER_USER_ID = "X-User-Id"
        const val HEADER_ACCEPT = "Accept"
        val TEXTUAL_MARKERS = listOf("json", "xml", "text/", "x-www-form-urlencoded")
        val STREAMING_MARKERS = listOf("octet-stream", "event-stream", "application/zip", "application/pdf", "application/gzip")
    }
}
