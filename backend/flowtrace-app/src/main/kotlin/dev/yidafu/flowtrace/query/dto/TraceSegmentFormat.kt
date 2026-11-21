package dev.yidafu.flowtrace.query.dto

/** `/v1/trace-segments` 的 geometry 输出格式（openapi `format` 参数）。 */
enum class TraceSegmentFormat(
    val value: String,
) {
    WKT("wkt"),
    GEOJSON("geojson"),
    ;

    companion object {
        fun fromValue(value: String): TraceSegmentFormat? = entries.find { it.value == value }
    }
}
