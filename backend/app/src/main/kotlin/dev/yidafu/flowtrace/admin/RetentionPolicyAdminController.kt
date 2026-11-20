package dev.yidafu.flowtrace.admin

import dev.yidafu.flowtrace.admin.dto.UpsertRetentionPolicyRequest
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/**
 * 保留策略管理控制器（docs/design/04-api.md §5，scope=trace:admin）。
 * 空实现占位：只定义路由契约，策略管理后续补充。
 *
 * 策略结构见 03 §5：scope=GLOBAL 或 USER 覆盖（USER 记录优先于 GLOBAL）。
 */
@RestController
@RequestMapping("/v1/admin/retention-policies")
class RetentionPolicyAdminController {
    /** 保留策略查询：GET /v1/admin/retention-policies */
    @GetMapping
    fun listPolicies(): ResponseEntity<Void> = TODO("待实现：保留策略列表查询")

    /** 保留策略更新：PUT /v1/admin/retention-policies（GET/PUT 同 body 结构）。 */
    @PutMapping
    fun upsertPolicy(
        @RequestBody request: UpsertRetentionPolicyRequest,
    ): ResponseEntity<Void> = TODO("待实现：保留策略创建/更新")
}
