package dev.yidafu.flowtrace.shared.web

import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.MDC
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import java.util.UUID

/**
 * 请求追踪过滤器：确定本次请求的 traceId，写入 MDC 并回显响应头。
 *
 * 取值优先级：客户端/网关传入的 `X-FlowTrace-Trace-Id` > 入站 `traceparent` 的 trace-id > 本地生成。
 * 顺序放在所有业务过滤器之前（[Ordered.HIGHEST_PRECEDENCE] + 10），保证后续日志与错误体都能带上同一 traceId。
 * 请求结束务必清理 MDC，否则虚拟线程复用/线程池复用时 traceId 会串。
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
class TraceIdFilter : OncePerRequestFilter() {
    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain,
    ) {
        val traceId = resolveTraceId(request)
        MDC.put(TraceContext.MDC_KEY, traceId)
        response.setHeader(TraceContext.TRACE_ID_HEADER, traceId)
        try {
            filterChain.doFilter(request, response)
        } finally {
            MDC.remove(TraceContext.MDC_KEY)
        }
    }

    private fun resolveTraceId(request: HttpServletRequest): String {
        request
            .getHeader(TraceContext.TRACE_ID_HEADER)
            ?.trim()
            ?.takeIf { it.isNotEmpty() }
            ?.let { return it }
        traceIdFromTraceparent(request.getHeader(TRACEPARENT))?.let { return it }
        return UUID.randomUUID().toString().replace("-", "")
    }

    /** W3C traceparent: `version-traceId-parentId-flags`，traceId 为 32 位小写十六进制。 */
    private fun traceIdFromTraceparent(header: String?): String? {
        val parts = header?.trim()?.split('-') ?: return null
        if (parts.size != 4) return null
        val traceId = parts[1]
        val valid = traceId.length == 32 && traceId.all { it.isDigit() || it in 'a'..'f' } && traceId.any { it != '0' }
        return traceId.takeIf { valid }
    }

    private companion object {
        const val TRACEPARENT = "traceparent"
    }
}
