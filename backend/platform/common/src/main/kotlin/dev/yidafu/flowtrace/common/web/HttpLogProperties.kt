package dev.yidafu.flowtrace.common.web

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

/**
 * 请求日志开关（`flowtrace.log.*`，07 §3）。
 *
 * 生产建议：`payload-on-error` 只在排查期打开——入参出参虽然已脱敏坐标，但仍含业务标识。
 */
@Component
@ConfigurationProperties(prefix = "flowtrace.log")
class HttpLogProperties {
    /** 是否输出请求访问日志（每个请求一行 INFO）。关闭后 4xx/5xx 的载荷日志也一并关闭。 */
    var requestEnabled: Boolean = true

    /** 4xx/5xx 时是否额外输出入参/出参（坐标已脱敏）。 */
    var payloadOnError: Boolean = true

    /** 单个请求体/响应体落日志的最大字符数，超出截断。 */
    var maxPayloadChars: Int = 2048
}
