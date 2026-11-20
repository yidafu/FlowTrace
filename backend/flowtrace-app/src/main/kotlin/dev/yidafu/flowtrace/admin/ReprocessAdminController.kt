package dev.yidafu.flowtrace.admin

import dev.yidafu.flowtrace.admin.dto.ReprocessRequest
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

/**
 * 重处理与死信控制器（docs/design/04-api.md §5，scope=trace:admin）。
 * 空实现占位：只定义路由契约，重处理/死信运维后续补充。
 */
@RestController
class ReprocessAdminController {
    /**
     * 触发重处理：POST /v1/admin/reprocess → jobId。
     *
     * [request.type] = "user-day"：按 user+day 重算段（写 dirty_day MANUAL，06 §4.6）
     * 或 "dlq"：重投指定死信批次（batchIds，06 §8）。
     */
    @PostMapping("/v1/admin/reprocess")
    fun reprocess(
        @RequestBody request: ReprocessRequest,
    ): ResponseEntity<Void> = TODO("待实现：重处理作业触发（幂等，返回 jobId）")

    /** 死信批列表：GET /v1/admin/dlq/batches?cursor=&reason=（batchId、device、错误、时间）。 */
    @GetMapping("/v1/admin/dlq/batches")
    fun listDlqBatches(
        @RequestParam("cursor", required = false) cursor: String?,
        @RequestParam("reason", required = false) reason: String?,
    ): ResponseEntity<Void> = TODO("待实现：死信批列表查询（游标分页）")
}
