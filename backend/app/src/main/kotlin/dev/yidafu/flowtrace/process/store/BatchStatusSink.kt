package dev.yidafu.flowtrace.process.store

import java.util.UUID

/**
 * 批台账状态写入出口（06 §3.2 第 8 步）。
 *
 * 抽成接口而不是让管道直接依赖 [BatchStatusUpdater]：异步缓冲是**实现细节**，
 * 管道只表达「这批算什么结局」。测试里用同步替身即可断言状态，
 * 不必为一个状态更新去等后台线程（那会让用例变成隐式 flaky）。
 */
interface BatchStatusSink {
    /** 处理成功：正常批、迟到批、重算锁让路批（点已落库）。 */
    fun markProcessed(batchId: UUID)

    /** 确定性失败，[errorCode] 落 `trace_batch.error_code`。 */
    fun markFailed(
        batchId: UUID,
        errorCode: String,
    )

    /** 进入死信（06 §3.7）。 */
    fun markDlq(
        batchId: UUID,
        errorCode: String,
    )
}
