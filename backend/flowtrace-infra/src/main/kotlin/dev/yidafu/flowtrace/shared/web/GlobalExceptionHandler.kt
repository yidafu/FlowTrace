package dev.yidafu.flowtrace.shared.web

import jakarta.servlet.http.HttpServletRequest
import jakarta.validation.ConstraintViolationException
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatusCode
import org.springframework.http.ResponseEntity
import org.springframework.http.converter.HttpMessageNotReadableException
import org.springframework.web.ErrorResponseException
import org.springframework.web.HttpMediaTypeNotSupportedException
import org.springframework.web.HttpRequestMethodNotSupportedException
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.MissingServletRequestParameterException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.method.annotation.HandlerMethodValidationException
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException
import org.springframework.web.servlet.resource.NoResourceFoundException

/**
 * 统一异常出口（04 §1.4）：把未捕获异常翻译成 `{"error": {...}}`，并记录错误日志。
 *
 * 入参/出参由 [HttpRequestLoggingFilter] 统一输出（那里才有请求体缓存），这里只补**根因与堆栈**：
 * 4xx 打 WARN（客户端问题，不需要堆栈），5xx 打 ERROR（带堆栈）。
 *
 * 注意：这里不打异常 message 的原始载荷——解码失败时异常消息可能夹带请求体片段，统一过 [PayloadMasking]。
 */
@RestControllerAdvice
class GlobalExceptionHandler {
    private val log = LoggerFactory.getLogger(GlobalExceptionHandler::class.java)

    @ExceptionHandler(HttpMessageNotReadableException::class)
    fun onMessageNotReadable(
        ex: HttpMessageNotReadableException,
        request: HttpServletRequest,
    ): ResponseEntity<ApiErrorResponse> {
        // 二进制协议解码失败按 INVALID_PROTOBUF；JSON 调试通道按 VALIDATION_ERROR
        val protobuf = request.contentType?.contains("protobuf", ignoreCase = true) == true
        val code = if (protobuf) ApiErrorCode.INVALID_PROTOBUF else ApiErrorCode.VALIDATION_ERROR
        return respond(HttpStatus.BAD_REQUEST, code, "请求体无法解析：${rootCauseMessage(ex)}", request, ex)
    }

    @ExceptionHandler(MethodArgumentNotValidException::class)
    fun onArgumentNotValid(
        ex: MethodArgumentNotValidException,
        request: HttpServletRequest,
    ): ResponseEntity<ApiErrorResponse> {
        val details =
            ex.bindingResult.fieldErrors
                .associate { it.field to (it.defaultMessage ?: "invalid") }
                .takeIf { it.isNotEmpty() }
        return respond(HttpStatus.BAD_REQUEST, ApiErrorCode.VALIDATION_ERROR, "参数校验失败", request, ex, details)
    }

    @ExceptionHandler(HandlerMethodValidationException::class)
    fun onMethodValidation(
        ex: HandlerMethodValidationException,
        request: HttpServletRequest,
    ): ResponseEntity<ApiErrorResponse> = respond(HttpStatus.BAD_REQUEST, ApiErrorCode.VALIDATION_ERROR, "参数校验失败", request, ex)

    @ExceptionHandler(ConstraintViolationException::class)
    fun onConstraintViolation(
        ex: ConstraintViolationException,
        request: HttpServletRequest,
    ): ResponseEntity<ApiErrorResponse> {
        val details = ex.constraintViolations.associate { it.propertyPath.toString() to it.message }.takeIf { it.isNotEmpty() }
        return respond(HttpStatus.BAD_REQUEST, ApiErrorCode.VALIDATION_ERROR, "参数校验失败", request, ex, details)
    }

    @ExceptionHandler(MissingServletRequestParameterException::class, MethodArgumentTypeMismatchException::class)
    fun onBadParameter(
        ex: Exception,
        request: HttpServletRequest,
    ): ResponseEntity<ApiErrorResponse> =
        respond(HttpStatus.BAD_REQUEST, ApiErrorCode.VALIDATION_ERROR, "参数缺失或类型不正确：${rootCauseMessage(ex)}", request, ex)

    @ExceptionHandler(HttpMediaTypeNotSupportedException::class)
    fun onUnsupportedMediaType(
        ex: HttpMediaTypeNotSupportedException,
        request: HttpServletRequest,
    ): ResponseEntity<ApiErrorResponse> =
        respond(HttpStatus.UNSUPPORTED_MEDIA_TYPE, ApiErrorCode.PROTOCOL_ERROR, "不支持的 Content-Type", request, ex)

    @ExceptionHandler(HttpRequestMethodNotSupportedException::class)
    fun onMethodNotSupported(
        ex: HttpRequestMethodNotSupportedException,
        request: HttpServletRequest,
    ): ResponseEntity<ApiErrorResponse> = respond(HttpStatus.METHOD_NOT_ALLOWED, ApiErrorCode.PROTOCOL_ERROR, "不支持的请求方法", request, ex)

    @ExceptionHandler(NoResourceFoundException::class)
    fun onNotFound(
        ex: NoResourceFoundException,
        request: HttpServletRequest,
    ): ResponseEntity<ApiErrorResponse> = respond(HttpStatus.NOT_FOUND, ApiErrorCode.NOT_FOUND, "资源不存在", request, ex)

    /** `throw new ResponseStatusException(...)` 等显式状态异常（Spring 6+ 统一为 ErrorResponseException）。 */
    @ExceptionHandler(ErrorResponseException::class)
    fun onErrorResponse(
        ex: ErrorResponseException,
        request: HttpServletRequest,
    ): ResponseEntity<ApiErrorResponse> {
        val status = HttpStatus.resolve(ex.statusCode.value()) ?: HttpStatus.INTERNAL_SERVER_ERROR
        val message = ex.body.detail ?: ex.message
        return respond(status, codeOf(status), message, request, ex)
    }

    @ExceptionHandler(Exception::class)
    fun onUnexpected(
        ex: Exception,
        request: HttpServletRequest,
    ): ResponseEntity<ApiErrorResponse> = respond(HttpStatus.INTERNAL_SERVER_ERROR, ApiErrorCode.INTERNAL, "服务内部错误", request, ex)

    private fun respond(
        status: HttpStatus,
        code: ApiErrorCode,
        message: String,
        request: HttpServletRequest,
        cause: Throwable? = null,
        details: Map<String, Any>? = null,
    ): ResponseEntity<ApiErrorResponse> {
        val safeMessage = PayloadMasking.truncate(PayloadMasking.mask(message), MAX_MESSAGE_CHARS)
        val body = apiError(code, safeMessage, details = details)
        val traceId = TraceContext.currentTraceId()
        if (status.is5xxServerError) {
            log.error(
                "接口异常 traceId={} code={} status={} method={} uri={} 出参={}",
                traceId,
                code.name,
                status.value(),
                request.method,
                request.requestURI,
                body.describe(),
                cause,
            )
        } else {
            log.warn(
                "接口异常 traceId={} code={} status={} method={} uri={} 出参={} cause={}",
                traceId,
                code.name,
                status.value(),
                request.method,
                request.requestURI,
                body.describe(),
                cause?.let { "${it.javaClass.simpleName}: ${it.message}" } ?: "-",
            )
        }
        return ResponseEntity.status(status).body(body)
    }

    private fun codeOf(status: HttpStatusCode): ApiErrorCode =
        when (status.value()) {
            400 -> ApiErrorCode.VALIDATION_ERROR
            401 -> ApiErrorCode.UNAUTHORIZED
            403 -> ApiErrorCode.FORBIDDEN
            404 -> ApiErrorCode.NOT_FOUND
            409 -> ApiErrorCode.CONFLICT
            413 -> ApiErrorCode.PAYLOAD_TOO_LARGE
            429 -> ApiErrorCode.RATE_LIMITED
            in 400..499 -> ApiErrorCode.PROTOCOL_ERROR
            else -> ApiErrorCode.INTERNAL
        }

    /** 取最内层异常消息，避免把包装异常的整串调用栈塞进响应。 */
    private fun rootCauseMessage(ex: Throwable): String {
        var current: Throwable = ex
        while (current.cause != null && current.cause !== current) current = current.cause!!
        return (current.message ?: current.javaClass.simpleName).lineSequence().first().take(MAX_MESSAGE_CHARS)
    }

    private companion object {
        const val MAX_MESSAGE_CHARS = 300
    }
}
