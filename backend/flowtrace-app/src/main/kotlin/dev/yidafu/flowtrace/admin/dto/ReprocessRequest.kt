package dev.yidafu.flowtrace.admin.dto

import java.time.LocalDate

/** 重处理请求体（POST /v1/admin/reprocess）：type=user-day 时填 userId/day，type=dlq 时填 batchIds。 */
data class ReprocessRequest(
    val type: String,
    val userId: Long? = null,
    val day: LocalDate? = null,
    val batchIds: List<String>? = null,
)
