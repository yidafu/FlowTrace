package dev.yidafu.flowtrace.common.web

import org.slf4j.MDC

/**
 * 请求追踪上下文（04 §1「请求追踪」/ 07 §5）。
 *
 * traceId 由 [TraceIdFilter] 在请求入口确定并写入 MDC，日志框架（含 JSON 格式化器）从 MDC 取 `traceId`；
 * 错误体也回填同一个值，便于按 traceId 把「客户端看到的错误」和「服务端日志」对上。
 */
object TraceContext {
    /** 回显给客户端的追踪头（W3C traceparent 派生，本服务自生成）。 */
    const val TRACE_ID_HEADER = "X-FlowTrace-Trace-Id"

    /** MDC 键名，与 07 §5 结构化日志字段 `traceId` 对齐。 */
    const val MDC_KEY = "traceId"

    fun currentTraceId(): String? = MDC.get(MDC_KEY)
}
