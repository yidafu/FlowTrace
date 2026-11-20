package dev.yidafu.flowtrace.shared.model

/**
 * 保留策略生效范围（03 §5 `retention_policy.scope_type`，int2）。
 *
 * - [GLOBAL]：全站默认；
 * - [USER]：单用户覆盖，**优先级高于** [GLOBAL]。
 */
enum class RetentionScope(
    val value: Short,
) {
    GLOBAL(1),
    USER(2),
    ;

    companion object {
        fun from(value: Short): RetentionScope =
            entries.firstOrNull { it.value == value }
                ?: throw IllegalArgumentException("未知保留策略范围: $value")
    }
}
