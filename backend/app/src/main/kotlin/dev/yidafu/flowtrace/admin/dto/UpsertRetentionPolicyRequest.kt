package dev.yidafu.flowtrace.admin.dto

/** 保留策略更新请求体（PUT /v1/admin/retention-policies；字段与默认值对应 03 §5 retention_policy 表）。 */
data class UpsertRetentionPolicyRequest(
    val scopeType: String,
    val userId: Long? = null,
    val hotPointDays: Int = 30,
    val segmentHotDays: Int = 180,
    val archiveEnabled: Boolean = true,
    val archiveKeepDays: Int = 365,
)
