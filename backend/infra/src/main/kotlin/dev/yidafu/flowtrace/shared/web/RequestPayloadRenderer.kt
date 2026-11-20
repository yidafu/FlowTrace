package dev.yidafu.flowtrace.shared.web

/**
 * 二进制请求体可读化扩展点。
 *
 * [HttpRequestLoggingFilter] 只认文本类 Content-Type；Protobuf 等二进制体由业务模块注册实现来「翻译」成单行摘要，
 * 好处是日志过滤器不必依赖任何协议库，业务模块也不必反向依赖日志实现。
 *
 * 实现方**必须**遵守 07 §5：不得输出坐标明文（用 [PayloadMasking] 或干脆不输出点位）。
 * 同名 bean 多个实现时按 `@Order` 排序，取第一个 [supports] 命中的。
 */
interface RequestPayloadRenderer {
    fun supports(contentType: String?): Boolean

    fun render(
        contentType: String?,
        body: ByteArray,
    ): String
}
