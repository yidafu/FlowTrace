package dev.yidafu.flowtrace.process.segment

import dev.yidafu.flowtrace.shared.model.MoveMode
import dev.yidafu.flowtrace.shared.model.TracePoint
import dev.yidafu.flowtrace.shared.model.TraceSegmentDraft

/**
 * 未闭合段的内存窗口（08 §8.1 / §8.2）。
 *
 * 保留**全部**点而非只留简化几何：STAY 是追溯判定的，DP 也要求整窗点；
 * 点数上限由 `ProcessorProperties.openSegMaxPoints` 控制，到顶强制分段。
 */
class OpenWindow(
    val startTsMs: Long,
    val points: MutableList<TracePoint>,
    var moveMode: MoveMode = MoveMode.UNKNOWN,
) {
    init {
        require(points.isNotEmpty()) { "OPEN 窗口至少有一个点" }
    }

    /** 窗口末点；拆段判定与清洗的上一保留点都基于它。 */
    val last: TracePoint
        get() = points.last()

    /**
     * 上一轮 flush 算出的段草稿（按 `startTsMs` 索引）。
     *
     * 只服务于「增量重算」：窗口是追加式的，新批只会改写尾部若干区间，其余区间的草稿
     * 可以原样复用（见 [SegmentBuilder.build] 的 `reusable`）。这份表**只用来省计算**：
     * 复用前提是区间首末点与点数完全相同，条件不满足就重算，不存在「用旧结果凑」的分支。
     */
    var drafts: Map<Long, TraceSegmentDraft> = emptyMap()
}
