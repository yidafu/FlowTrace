package dev.yidafu.flowtrace.model

import java.util.UUID

/**
 * 轨迹段草稿：`SegmentBuilder` 的纯计算结果，尚未落库。
 *
 * 与 `trace_segment` 表行的差别只在存储侧字段：[SegmentStatus]、`updated_at` 由写入层按
 * 水位/当前时刻填充，
 * 不进入草稿——保证「同一批点重算两次得到完全相同的草稿」，这是幂等重放的前提。
 *
 * [segmentId] 由 `UUIDv5(FLOWTRACE_SEGMENT_NS, "{userId}:{sessionId}:{startTsMs}")` 派生（02 §7 / 决策 D8），
 * 确定性意味着段可以 upsert：段被后续点延长时 `startTsMs` 不变 → id 不变 → 更新而非新增。
 */
data class TraceSegmentDraft(
    val segmentId: UUID,
    val startTsMs: Long,
    val endTsMs: Long,
    val kind: SegmentKind,
    val moveMode: MoveMode,
    val pointCount: Int,
    val distanceM: Double,
    val geometryWkt: String,
    val bbox: Bbox,
    val stayLatQ: Int? = null,
    val stayLngQ: Int? = null,
) {
    init {
        require(endTsMs >= startTsMs) { "段结束时间不得早于开始时间: start=$startTsMs, end=$endTsMs" }
        require(pointCount > 0) { "段内点数必须为正: $pointCount" }
        // STAY 段必须有质心坐标（消费方免解析 WKT 直接取用）；MOVE 段不该有
        require((kind == SegmentKind.STAY) == (stayLatQ != null && stayLngQ != null)) {
            "STAY 段必须且只有 STAY 段带质心坐标: kind=$kind, stayLatQ=$stayLatQ, stayLngQ=$stayLngQ"
        }
    }

    /** 段时长（毫秒），`trace_segment` 不存该列，按需计算。 */
    val durationMs: Long
        get() = endTsMs - startTsMs
}
