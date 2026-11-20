package dev.yidafu.flowtrace.admin

import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Instant

/**
 * 审计控制器（docs/design/04-api.md §5，scope=trace:admin）。
 * 空实现占位：只定义路由契约，审计查询后续补充。
 *
 * 审计动作（03 §5）：USER_TRACES_DELETE / DEVICE_DISABLE / POLICY_UPDATE /
 * REPROCESS / PARTITION_DROP / ARCHIVE_RUN / DLQ_REPLAY。
 */
@RestController
@RequestMapping("/v1/admin/audit")
class AuditAdminController {
    /** 审计查询：GET /v1/admin/audit?actor=&action=&start=&end=&cursor= */
    @GetMapping
    fun listAuditLogs(
        @RequestParam("actor", required = false) actor: String?,
        @RequestParam("action", required = false) action: String?,
        @RequestParam("start", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) start: Instant?,
        @RequestParam("end", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) end: Instant?,
        @RequestParam("cursor", required = false) cursor: String?,
    ): ResponseEntity<Void> = TODO("待实现：审计日志查询（audit_log 按月分区，游标分页）")
}
