package dev.yidafu.flowtrace.model

/**
 * 轨迹段类型（03 §3 `trace_segment.kind`）：
 * - [MOVE]：移动段，geometry 为 LINESTRING（DP 抽稀后）；
 * - [STAY]：停留点，geometry 为质心 POINT，`distance_m = 0`（06 §3.5）。
 */
enum class SegmentKind(
    val value: Short,
) {
    MOVE(1),
    STAY(2),
    ;

    companion object {
        fun from(value: Short): SegmentKind =
            entries.firstOrNull { it.value == value }
                ?: throw IllegalArgumentException("未知轨迹段类型: $value")
    }
}
