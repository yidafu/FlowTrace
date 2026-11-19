package dev.yidafu.flowtrace.model

/**
 * 补录重算任务状态（03 §5 `dirty_day.status`，int2）。
 *
 * 流转：PENDING →（claim）→ RUNNING → DONE / FAILED；
 * 失败未达上限时回到 PENDING 重排队（06 §3.6），达上限置 FAILED 等人工介入。
 */
enum class DirtyDayStatus(
    val value: Short,
) {
    PENDING(1),
    RUNNING(2),
    DONE(3),
    FAILED(4),
    ;

    companion object {
        fun from(value: Short): DirtyDayStatus =
            entries.firstOrNull { it.value == value }
                ?: throw IllegalArgumentException("未知重算状态: $value")
    }
}
