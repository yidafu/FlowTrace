package dev.yidafu.flowtrace.model

/**
 * 定位来源（02 §2 `trace.proto` 的 `LocationSource` 枚举）。
 *
 * 数值与协议一致，直接落 `trace_point.source`（int2），不做转换。
 */
enum class LocationSource(
    val value: Int,
) {
    UNSPECIFIED(0),
    GPS(1),
    NETWORK(2),
    FUSED(3),
    MANUAL(4),
    ;

    companion object {
        /**
         * 由协议值转枚举：未知值（未来新增来源）归入 [UNSPECIFIED]，保持前向兼容，
         * 不因一个未知来源整批失败（02 §3「未知 flags 位忽略」同一原则）。
         * 注意协议约定「缺省按 FUSED 处理」是**端侧**语义，落到库里的 0 就是 0，不做替换。
         */
        fun fromProto(value: Int): LocationSource = entries.firstOrNull { it.value == value } ?: UNSPECIFIED

        /** 由库值转枚举：未知值返回 null，供读取侧决定降级策略。 */
        fun fromStored(value: Int): LocationSource? = entries.firstOrNull { it.value == value }
    }
}
