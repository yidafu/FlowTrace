package dev.yidafu.flowtrace.common.web

/**
 * 统一错误体（docs/design/04-api.md §1.4）：
 *
 * ```json
 * { "error": { "code": "RATE_LIMITED", "message": "...", "retryAfterSec": 60,
 *              "traceId": "00-4bf9...-01", "details": { "nextUploadIntervalSec": 300 } } }
 * ```
 *
 * 所有服务共用：控制器显式返回错误时用 [apiError]，未捕获异常由 [GlobalExceptionHandler] 兜底。
 */
data class ApiErrorResponse(
    val error: ApiError,
)

data class ApiError(
    val code: String,
    val message: String,
    val retryAfterSec: Long? = null,
    val traceId: String? = TraceContext.currentTraceId(),
    val details: Map<String, Any>? = null,
)

/** 错误码全集（04 §1.4）。新增码必须先改文档。 */
enum class ApiErrorCode {
    UNAUTHORIZED,
    FORBIDDEN,
    DEVICE_DISABLED,
    INVALID_GZIP,
    INVALID_PROTOBUF,
    INVALID_SCHEMA_VERSION,
    TS_OUT_OF_WINDOW,
    INVALID_COORD,
    TS_NOT_SORTED,
    PROTOCOL_ERROR,
    PAYLOAD_TOO_LARGE,
    RATE_LIMITED,
    NOT_FOUND,
    VALIDATION_ERROR,
    CONFLICT,
    INTERNAL,
}

/** 构造错误体；traceId 默认取当前请求的 MDC 值。 */
fun apiError(
    code: ApiErrorCode,
    message: String,
    retryAfterSec: Long? = null,
    details: Map<String, Any>? = null,
): ApiErrorResponse =
    ApiErrorResponse(
        ApiError(
            code = code.name,
            message = message,
            retryAfterSec = retryAfterSec,
            traceId = TraceContext.currentTraceId(),
            details = details,
        ),
    )

/** 供日志使用的单行渲染（避免为打日志再引入序列化器）。 */
fun ApiErrorResponse.describe(): String =
    buildString {
        append("error{code=").append(error.code)
        append(", message=").append(error.message)
        error.retryAfterSec?.let { append(", retryAfterSec=").append(it) }
        append(", traceId=").append(error.traceId ?: "-")
        error.details?.let { append(", details=").append(it) }
        append('}')
    }
