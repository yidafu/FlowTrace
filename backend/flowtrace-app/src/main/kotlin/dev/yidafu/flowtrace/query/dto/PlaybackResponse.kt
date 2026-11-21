package dev.yidafu.flowtrace.query.dto

/**
 * 轨迹回放响应（04 §3 / openapi `PlaybackResponse`），固定 compact 形态。
 *
 * [points] 每项定长 3 或 5：
 * - `withAttrs=false` → `[tsMs, latE7, lngE7]`
 * - `withAttrs=true` → `[tsMs, latE7, lngE7, speedMps, bearingDeg]`
 *
 * 元素用 [Number] 而不是统一 Double：时间戳与量化坐标必须是 JSON 整数，
 * 否则前端拿到的 epoch ms 带小数尾巴。实际装入 Long / Float / Short，
 * Jackson 按运行时类型输出。速度/航向缺失按协议口径填 0（`0 = 缺省`）。
 */
data class PlaybackResponse(
    val userId: String,
    val start: String,
    val end: String,
    val zoom: Int,
    val withAttrs: Boolean,
    val points: List<List<Number>>,
    val returnedPoints: Int,
    val originalPoints: Int,
)
