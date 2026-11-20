package dev.yidafu.flowtrace.realtime

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController

/**
 * 实时位置控制器（docs/design/04-api.md §3 `/v1/users/{userId}/location`）。
 * 空实现占位：只定义路由契约，Redis 读写后续补充。
 */
@RestController
class RealtimeLocationController {
    /**
     * 查询用户最新位置（来源：Redis 最新位置缓存）。
     * 无数据 → 404 NOT_FOUND；stale = updatedAt 距服务端当前时间 > 5min（STALE_MS=300000）。
     */
    @GetMapping("/v1/users/{userId}/location")
    fun latestLocation(
        @PathVariable("userId") userId: Long,
    ): ResponseEntity<Void> = TODO("待实现：Redis 最新位置读取（含 stale 判定）")
}
