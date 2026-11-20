package dev.yidafu.flowtrace.query

import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Instant

/**
 * 轨迹删除与作业控制器（docs/design/04-api.md §4，scope=trace:admin）。
 * 空实现占位：只定义路由契约，删除作业与作业查询后续补充。
 */
@RestController
class TraceDeleteJobController {
    /**
     * 删除用户轨迹：DELETE /v1/users/{userId}/traces（隐私撤回）。
     *
     * [start]/[end] 可选，缺省全量删除；end 独占。
     * 响应 202 {jobId, status: QUEUED}；作业幂等——同用户存在运行中作业则返回该 jobId。
     * 执行覆盖所有存储（trace_point / trace_segment / trace_batch / Parquet 冷数据 / Redis）。
     */
    @DeleteMapping("/v1/users/{userId}/traces")
    fun deleteTraces(
        @PathVariable("userId") userId: Long,
        @RequestParam("start", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) start: Instant?,
        @RequestParam("end", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) end: Instant?,
    ): ResponseEntity<Void> = TODO("待实现：异步删除作业（幂等），覆盖所有存储")

    /**
     * 作业状态查询：GET /v1/jobs/{jobId}。
     * type 包括 USER_TRACES_DELETE / REPROCESS / ARCHIVE_RUN；
     * status：QUEUED / RUNNING / DONE / FAILED(可重跑)。
     */
    @GetMapping("/v1/jobs/{jobId}")
    fun getJob(
        @PathVariable("jobId") jobId: String,
    ): ResponseEntity<Void> = TODO("待实现：作业状态查询（含 progress）")
}
