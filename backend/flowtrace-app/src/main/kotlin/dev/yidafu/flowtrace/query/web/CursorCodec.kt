package dev.yidafu.flowtrace.query.web

import org.springframework.stereotype.Component
import tools.jackson.module.kotlin.jacksonObjectMapper
import java.util.Base64

/** 游标解码失败：控制器据此返回 400 VALIDATION_ERROR，而不是让 Jackson 异常冒泡成 500。 */
class InvalidCursorException(
    message: String,
    cause: Throwable? = null,
) : IllegalArgumentException(message, cause)

/**
 * 分页游标编解码（04 §1：base64url 不透明串）。
 *
 * 游标对象本身定义在仓储层（`TracePointCursor` / `TraceSegmentCursor`），
 * 那里是 `(ts, id)` keyset 位置的唯一事实源；这里只负责序列化，不再另建同名 DTO。
 */
@Component
class CursorCodec {
    /** Jackson 3 的 Kotlin 模块已注册，UUID / Long 可直接往返。 */
    val objectMapper = jacksonObjectMapper()

    fun encode(obj: Any): String {
        val json = objectMapper.writeValueAsString(obj)
        return Base64
            .getUrlEncoder()
            .withoutPadding()
            .encodeToString(json.toByteArray(Charsets.UTF_8))
    }

    fun <T> decode(
        s: String,
        type: Class<T>,
    ): T =
        try {
            val bytes =
                Base64
                    .getUrlDecoder()
                    .decode(s)
            objectMapper.readValue(bytes, type)
        } catch (ex: Exception) {
            throw InvalidCursorException("非法分页游标", ex)
        }
}
