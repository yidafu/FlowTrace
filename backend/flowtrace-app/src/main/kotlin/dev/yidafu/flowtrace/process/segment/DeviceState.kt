package dev.yidafu.flowtrace.process.segment

import dev.yidafu.flowtrace.shared.model.TracePoint

/**
 * 单设备跨批状态（08 §8.1）。
 *
 * 不是 Spring bean：由 [DeviceStateStore] 在缓存 miss 时构造，生命周期跟随 LRU。
 * 全部字段可变——清洗器更新 [lastKept]，分段器更新 [open] / [tailFinalized] / [openTruncated]。
 *
 * 这里只放「跨批必须记住的东西」：写库需要的身份与段状态都不在这里，
 * 身份以批头为准（见 [dev.yidafu.flowtrace.process.pipeline.BatchMeta]），
 * 段的删除范围按窗口起点收敛（`replaceTail`），不需要记住写过哪些 segmentId。
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
    /**
     * 最近一次 flush 是否已把 [open] 写成 `FINALIZED`。
     *
     * 用于批末的写入门控：内容没变（本批没有点进窗口）且尾段已封口时，再 flush 一次
     * 只会把同一批行按同一结果覆盖一遍，纯属白付「整窗重算 + replaceTail」。
     * 追加新点即复位为 false（内容变了，状态要跟着重算）。
     */
    var tailFinalized: Boolean = false,
    /**
     * 恢复时窗口被 `openSegMaxPoints` 截断：旧 OPEN 段的起点已不在内存窗口内，
     * 不能再按内存首点做 `replaceTail`（会漏删旧段）。flush 时须先把旧段 FINALIZE。
     */
    var openTruncated: Boolean = false,
)
