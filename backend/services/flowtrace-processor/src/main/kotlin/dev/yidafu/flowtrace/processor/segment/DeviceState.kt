package dev.yidafu.flowtrace.processor.segment

import dev.yidafu.flowtrace.model.TracePoint
import java.util.UUID

/**
 * 单设备跨批状态（08 §8.1）。
 *
 * 不是 Spring bean：由 [DeviceStateStore] 在缓存 miss 时构造，生命周期跟随 LRU。
 * 全部字段可变——清洗器更新 [lastKept]，分段器更新 [open] 与 [writtenSegmentIds]。
 */
class DeviceState(
    val deviceId: String,
    var userId: String? = null,
    var sessionId: String? = null,
    var taskId: String? = null,
    /** 清洗规则的跨批基准：速度/精度/完全重复都要和上一保留点比。 */
    var lastKept: TracePoint? = null,
    /** 未闭合段；为 null 表示当前没有可扩展的段。 */
    var open: OpenWindow? = null,
    /** 本 OPEN 窗口已经写过的 segmentId，尾段重写时用于收敛删除范围。 */
    val writtenSegmentIds: MutableSet<UUID> = mutableSetOf(),
    /**
     * 恢复时窗口被 `openSegMaxPoints` 截断：旧 OPEN 段的起点已不在内存窗口内，
     * 不能再按内存首点做 `replaceTail`（会漏删旧段）。flush 时须先把旧段 FINALIZE。
     */
    var openTruncated: Boolean = false,
)
