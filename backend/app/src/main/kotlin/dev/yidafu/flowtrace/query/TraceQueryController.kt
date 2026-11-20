package dev.yidafu.flowtrace.query

import org.springframework.format.annotation.DateTimeFormat
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Instant

/**
 * 轨迹查询控制器（docs/design/04-api.md §3）：轨迹点查询、轨迹段/停留点、回放、里程。
 * 空实现占位：只定义路由契约与查询参数，查询逻辑后续补充。
 *
 * 通用约定：时间参数 ISO-8601 UTC，end 独占；分页游标式（cursor/nextCursor）；
 * 坐标空间计算（抽稀/里程）全部在应用层 JTS 完成，不做数据库地理查询。
 */
@RestController
class TraceQueryController {
    /**
     * 轨迹点查询：GET /v1/traces（默认简化轨迹）。
     *
     * [simplify] 默认 true；false 时忽略 zoom/maxPoints 返回原始点（仍受 limit 分页约束）。
     * [zoom] 0..21，默认 15，简化 LUT 见 06 §5.3。
     * [maxPoints] 默认 2000，简化后仍超则等距抽取。
     * [format] geojson|compact，默认 geojson（compact 为 [tsMs, latE7, lngE7] 整数三元组）。
     * [limit] 默认 2000，上限 5000。跨度约束：end-start ≤ 7 天。
     */
    @GetMapping("/v1/traces")
    fun listTraces(
        @RequestParam("userId") userId: Long,
        @RequestParam("start") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) start: Instant,
        @RequestParam("end") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) end: Instant,
        @RequestParam("simplify", defaultValue = "true") simplify: Boolean,
        @RequestParam("zoom", defaultValue = "15") zoom: Int,
        @RequestParam("maxPoints", defaultValue = "2000") maxPoints: Int,
        @RequestParam("format", defaultValue = "geojson") format: String,
        @RequestParam("limit", defaultValue = "2000") limit: Int,
        @RequestParam("cursor", required = false) cursor: String?,
    ): ResponseEntity<Void> = TODO("待实现：(user_id, ts) 索引查询 + JTS 抽稀，返回 geojson|compact + nextCursor")

    /**
     * 轨迹段/停留点查询：GET /v1/trace-segments。跨度约束：end-start ≤ 31 天。
     *
     * [kind] move|stay|all，默认 all。
     * [sessionId]/[taskId] 巡检业务关联过滤（业务经此关联轨迹，不侵入上报主链路）。
     * [format] wkt|geojson，默认 wkt。
     */
    @GetMapping("/v1/trace-segments")
    fun listSegments(
        @RequestParam("userId") userId: Long,
        @RequestParam("start") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) start: Instant,
        @RequestParam("end") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) end: Instant,
        @RequestParam("kind", defaultValue = "all") kind: String,
        @RequestParam("sessionId", required = false) sessionId: String?,
        @RequestParam("taskId", required = false) taskId: String?,
        @RequestParam("format", defaultValue = "wkt") format: String,
        @RequestParam("limit", defaultValue = "500") limit: Int,
        @RequestParam("cursor", required = false) cursor: String?,
    ): ResponseEntity<Void> = TODO("待实现：trace_segment 查询（geometry 文本列直读，无需空间函数）")

    /**
     * 轨迹回放：GET /v1/traces/playback（按缩放级别取点）。跨度约束：end-start ≤ 24h。
     *
     * 等价于 /v1/traces 简化管道 + 回放专用 LUT（06 §5.4），返回 compact 数组；
     * [withAttrs] true 时每点附 speed/bearing 可选字段。
     */
    @GetMapping("/v1/traces/playback")
    fun playback(
        @RequestParam("userId") userId: Long,
        @RequestParam("start") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) start: Instant,
        @RequestParam("end") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) end: Instant,
        @RequestParam("zoom", defaultValue = "15") zoom: Int,
        @RequestParam("maxPoints", defaultValue = "500") maxPoints: Int,
        @RequestParam("withAttrs", defaultValue = "false") withAttrs: Boolean,
    ): ResponseEntity<Void> = TODO("待实现：回放取点（compact + 可选 speed/bearing）")

    /**
     * 里程计算：GET /v1/traces/distance。跨度约束：end-start ≤ 31 天。
     *
     * [byMode] 默认 true，按出行模式分组。口径：段表求和（kind=MOVE）；
     * 范围内存在 OPEN 段或缺失段时回退原始点逐段 haversine（source="points"，
     * 响应头 X-FlowTrace-Degraded: true）。
     */
    @GetMapping("/v1/traces/distance")
    fun distance(
        @RequestParam("userId") userId: Long,
        @RequestParam("start") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) start: Instant,
        @RequestParam("end") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) end: Instant,
        @RequestParam("byMode", defaultValue = "true") byMode: Boolean,
    ): ResponseEntity<Void> = TODO("待实现：JTS/haversine 里程计算")
}
