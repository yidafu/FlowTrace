package dev.yidafu.flowtrace.query.dto

/** 里程计算口径（openapi `DistanceResponse.source`）。 */
enum class DistanceSource(
    val value: String,
) {
    SEGMENTS("segments"),
    POINTS("points"),
    ;

    companion object {
        fun fromValue(value: String): DistanceSource? = entries.find { it.value == value }
    }
}

/**
 * 里程响应（04 §3 / openapi `DistanceResponse`）。
 *
 * [byMode] 在 `byMode=false` 或降级到点级口径时为空 map（openapi 允许「省略或为空」），
 * 避免 null 与 Jackson 的序列化配置耦合。
 */
data class DistanceResponse(
    val totalM: Double,
    val byMode: Map<String, Double>,
    val source: String,
)
