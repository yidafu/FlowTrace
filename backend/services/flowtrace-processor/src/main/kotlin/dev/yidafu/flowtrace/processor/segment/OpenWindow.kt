package dev.yidafu.flowtrace.processor.segment

import dev.yidafu.flowtrace.model.MoveMode
import dev.yidafu.flowtrace.model.TracePoint

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
}
