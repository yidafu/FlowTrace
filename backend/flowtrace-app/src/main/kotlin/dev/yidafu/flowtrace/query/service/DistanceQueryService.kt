package dev.yidafu.flowtrace.query.service

import dev.yidafu.flowtrace.query.dto.DistanceResponse
import dev.yidafu.flowtrace.query.dto.DistanceSource
import dev.yidafu.flowtrace.shared.model.GeoMath
import dev.yidafu.flowtrace.shared.model.TracePoint
import dev.yidafu.flowtrace.shared.persistence.repository.TracePointRepository
import dev.yidafu.flowtrace.shared.persistence.repository.TraceSegmentRepository
import org.springframework.stereotype.Service
import java.time.Instant

/** 里程结果 + 是否走降级口径（控制器据此设置 `X-FlowTrace-Degraded` 响应头）。 */
data class DistanceQueryResult(
    val response: DistanceResponse,
    val degraded: Boolean,
)

/**
 * 里程查询（04 §3 / 06 §5.1）：优先段表求和，段数据不可信时回退点级 haversine。
 *
 * 段口径只聚合 `kind=MOVE`（STAY 的 distance_m 恒为 0）。降级判据：
 * 1. 范围内存在 OPEN 段——尾段仍会增长，求和偏差未知；
 * 2. 范围内有点、却一个 MOVE 段都没有——典型是段表上线前的早期数据（缺失段）。
 *
 * 降级后按 `(deviceId, sessionId)` 分组逐点累加：段本身按这个身份范围生成，
 * 跨设备/跨会话的相邻点不构成一段轨迹，直接全序累加入里程会凭空多出跳变距离。
 */
@Service
class DistanceQueryService(
    private val segmentRepository: TraceSegmentRepository,
    private val pointRepository: TracePointRepository,
    private val traceQueryService: TraceQueryService,
) {
    fun distance(
        userId: String,
        start: Instant,
        end: Instant,
        byMode: Boolean,
    ): DistanceQueryResult {
        val fromMs = start.toEpochMilli()
        val toMs = end.toEpochMilli()
        val sums = segmentRepository.sumDistanceByMode(userId, fromMs, toMs)
        val missingSegments =
            sums.isEmpty() && pointRepository.countByUser(userId, fromMs, toMs) > 0
        if (segmentRepository.hasOpenInRange(userId, fromMs, toMs) || missingSegments) {
            return DistanceQueryResult(
                response =
                    DistanceResponse(
                        totalM = distanceFromPoints(userId, start, end),
                        byMode = emptyMap(),
                        source = DistanceSource.POINTS.value,
                    ),
                degraded = true,
            )
        }
        return DistanceQueryResult(
            response =
                DistanceResponse(
                    totalM = sums.values.sum(),
                    // 与 /v1/trace-segments 的 moveMode 保持一致，输出领域枚举名；
                    // openapi 示例写 BIKE 而领域是 BICYCLE，口径未对齐前不做单点转换。
                    byMode = if (byMode) sums.mapKeys { it.key.name } else emptyMap(),
                    source = DistanceSource.SEGMENTS.value,
                ),
            degraded = false,
        )
    }

    private fun distanceFromPoints(
        userId: String,
        start: Instant,
        end: Instant,
    ): Double {
        var total = 0.0
        val previous = HashMap<Pair<String, String?>, TracePoint>()
        traceQueryService.readWindow(userId, start, end).forEach { point ->
            val key = point.deviceId to point.sessionId
            previous[key]?.let { total += GeoMath.distanceM(it, point) }
            previous[key] = point
        }
        return total
    }
}
