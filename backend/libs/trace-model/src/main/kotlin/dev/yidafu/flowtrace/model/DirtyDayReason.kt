package dev.yidafu.flowtrace.model

/**
 * 补录重算入队原因（03 §5 `dirty_day.reason`，int2）。
 *
 * - [LATE_DATA]：processor 发现迟到批（`ts < 水位 − 10min`）自动入队（06 §3.2 第 6 步）；
 * - [MANUAL]：admin 触发 `POST /v1/admin/reprocess`（type=user-day）。
 */
enum class DirtyDayReason(
    val value: Short,
) {
    LATE_DATA(1),
    MANUAL(2),
    ;

    companion object {
        fun from(value: Short): DirtyDayReason =
            entries.firstOrNull { it.value == value }
                ?: throw IllegalArgumentException("未知重算原因: $value")
    }
}
