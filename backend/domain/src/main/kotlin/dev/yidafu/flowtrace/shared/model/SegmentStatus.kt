package dev.yidafu.flowtrace.shared.model

/**
 * 轨迹段状态（03 §3 `trace_segment.status`）：
 * - [OPEN]：水位之上，仍可能被后续点扩展（processor 在线分段的尾段）；
 * - [FINALIZED]：已收尾，不会再变（`end_ts < 水位 − 10min` 或离线重算产物）。
 *
 * 段是派生数据（可由 `trace_point` 重算），状态只是「是否还能增长」的标记，
 * 不参与幂等判定——幂等靠 `segment_id` 确定性派生 + upsert。
 */
enum class SegmentStatus(
    val value: Short,
) {
    OPEN(1),
    FINALIZED(2),
    ;

    companion object {
        fun from(value: Short): SegmentStatus =
            entries.firstOrNull { it.value == value }
                ?: throw IllegalArgumentException("未知轨迹段状态: $value")
    }
}
