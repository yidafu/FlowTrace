package dev.yidafu.flowtrace.shared.model

/**
 * 上报批次处理状态（03 §4 `trace_batch.status`，int2）。
 *
 * - [ACCEPTED]：upload 写入的初始状态（Kafka 已确认收下）；
 * - [PROCESSED]：processor 处理成功；
 * - [FAILED]：确定性失败（含 `TOMBSTONE` 丢弃），可经 admin 重处理；
 * - [DLQ]：重试耗尽后进死信，`error_code` 记原因（06 §3.7）。
 *
 * 失败扫描索引 `trace_batch_failed_idx` 只覆盖 (3, 4) 两种状态，
 * 因此 [isFailure] 的口径必须与索引条件保持一致。
 */
enum class TraceBatchStatus(
    val value: Short,
) {
    ACCEPTED(1),
    PROCESSED(2),
    FAILED(3),
    DLQ(4),
    ;

    /** 是否属于失败态（失败批扫描/重处理的唯一判据）。 */
    val isFailure: Boolean
        get() = this == FAILED || this == DLQ

    companion object {
        fun from(value: Short): TraceBatchStatus =
            entries.firstOrNull { it.value == value }
                ?: throw IllegalArgumentException("未知批次状态值: $value")
    }
}
