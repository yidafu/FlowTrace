package dev.yidafu.flowtrace.shared.model

/**
 * 运动模式（03 §3 `trace_segment.move_mode`）。
 *
 * 只承载数值口径，不含判定逻辑：速度阈值可配（`flowtrace.processor`），
 * 推断与段内多数投票属 processor 的 `MoveModeClassifier`（06 §3.4），
 * 放在模型层会与配置项形成第二份事实源。
 */
enum class MoveMode(
    val value: Short,
) {
    UNKNOWN(0),
    STILL(1),
    WALK(2),
    RUN(3),
    BICYCLE(4),
    VEHICLE(5),
    ;

    companion object {
        fun from(value: Short): MoveMode =
            entries.firstOrNull { it.value == value }
                ?: throw IllegalArgumentException("未知运动模式: $value")
    }
}
