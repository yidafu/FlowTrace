package dev.yidafu.flowtrace.model

/**
 * 点级标志位（02 §2 `trace.proto` 顶部，位掩码可组合），落 `trace_point.flags`（int4，缺省 0）。
 *
 * 未知位一律忽略（前向兼容，02 §3）：判定只能 `flags and MASK != 0`，不能用等值比较。
 */
object PointFlags {
    /** 进入巡检点/围栏半径（关键点，触发实时上报）。 */
    const val ENTER_RADIUS: Int = 0x01

    /** 离开巡检点/围栏半径。 */
    const val EXIT_RADIUS: Int = 0x02

    /** 运动状态切换（processor 分段时优先采信）。 */
    const val MOTION_CHANGE: Int = 0x04

    /** 强制保留点（关键点优先策略，清洗阶段不应丢弃）。 */
    const val FORCED: Int = 0x08

    /** 低电量模式下采集（采样已降频，属于质量提示）。 */
    const val LOW_BATTERY: Int = 0x10
}
