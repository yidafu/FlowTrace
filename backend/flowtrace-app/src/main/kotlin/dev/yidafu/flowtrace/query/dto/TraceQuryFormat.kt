package dev.yidafu.flowtrace.query.dto

enum class TraceQueryFormat(val value: String) {
    GEOJSON("geojson"),
    COMPACT("compact");
    companion object {
        fun fromValue(value: String): TraceQueryFormat? = entries.find { it.value == value }
    }
}