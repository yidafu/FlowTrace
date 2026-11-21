package dev.yidafu.flowtrace.query.service

import dev.yidafu.flowtrace.query.dto.PlaybackResponse
import dev.yidafu.flowtrace.query.web.TraceSimplifier
import dev.yidafu.flowtrace.shared.model.TracePoint
import dev.yidafu.flowtrace.shared.persistence.repository.TracePointRepository
import org.springframework.stereotype.Service
import java.time.Instant

/**
 * 回放取点（04 §3 / 06 §5.1）：整窗读原始点 → LUT 简化 → `maxPoints` 兜底抽取。
 *
 * 文档所称「回放专用 LUT（06 §5.4）」在 06 中并无独立表格（§5.4 是 P95 预算），
 * 因此复用 [TraceSimplifier] 的 §5.2 zoom→ε LUT，避免维护第二份阈值口径。
 *
 * 与 `/v1/traces` 的区别：回放没有游标参数，要覆盖完整 24h 窗口，故走 [TraceQueryService.readWindow]
 * 流式读整窗；`originalPoints` 取窗口真实点数（openapi 示例口径），不是本页点数。
 */
@Service
class PlaybackQueryService(
    private val traceQueryService: TraceQueryService,
    private val pointRepository: TracePointRepository,
    private val simplifier: TraceSimplifier,
) {
    fun playback(
        userId: String,
        start: Instant,
        end: Instant,
        zoom: Int,
        maxPoints: Int,
        withAttrs: Boolean,
    ): PlaybackResponse {
        val originalPoints =
            pointRepository
                .countByUser(userId, start.toEpochMilli(), end.toEpochMilli())
                .coerceAtMost(Int.MAX_VALUE.toLong())
                .toInt()
        // 极端高频单设备保护：超过上限只回放前 N 点，避免把整窗原始点驻留堆内存。
        val points = traceQueryService.readWindow(userId, start, end).take(MAX_SOURCE_POINTS).toList()
        val output = simplifier.simplify(points, zoom, maxPoints)
        return PlaybackResponse(
            userId = userId,
            start = start.toString(),
            end = end.toString(),
            zoom = zoom,
            withAttrs = withAttrs,
            points = output.map { it.toCompact(withAttrs) },
            returnedPoints = output.size,
            originalPoints = originalPoints,
        )
    }

    private fun TracePoint.toCompact(withAttrs: Boolean): List<Number> =
        if (withAttrs) {
            listOf<Number>(tsMs, latQ.toLong(), lngQ.toLong(), speedMps ?: 0f, bearingDeg ?: 0)
        } else {
            listOf<Number>(tsMs, latQ.toLong(), lngQ.toLong())
        }

    private companion object {
        /** 24h 回放的源点硬上限：按 1s 高频全量采集也留有余量（86,400 点/天）。 */
        const val MAX_SOURCE_POINTS = 100_000
    }
}
