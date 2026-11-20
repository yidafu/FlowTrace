package dev.yidafu.flowtrace.shared.protocol.delta

import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.v1.TraceBatchHeader
import dev.yidafu.flowtrace.v1.TraceBatchRequest

/**
 * Delta 解码器（06 §1.2、02 §4.3）：批头基准值 + 链式增量 → 绝对坐标点列。
 *
 * 编码契约（proto/trace.proto）：`cur = prev + delta`，`prev₀ = header.base`。
 * 所以**必须逐点滚动累加**，不能把 `d_lat` 当成绝对坐标用。
 *
 * 本类只做展开，不做判断：是否合法由 [dev.yidafu.flowtrace.shared.protocol.validation.BatchValidator] 决定。
 * 唯一例外是记录「第一个 `d_ts < 0` 的下标」——这是解码过程中唯一无法事后还原的信息
 * （累加之后看不出是被哪个增量拉回去的），而它对应 422 的 `TS_NOT_SORTED`。
 *
 * **idx 保留批内原始下标**：`pointId = UUIDv5(batchId, idx)`（02 §7），折叠重复点时若重排下标，
 * 同一批数据重放两次会得到不同的 pointId，点级幂等直接失效。
 */
class DeltaBatchDecoder {
    /**
     * @return 绝对坐标点列（未折叠）+ 首个负 `d_ts` 下标（null 表示全批单调不减）
     */
    fun decode(request: TraceBatchRequest): DecodedBatch {
        val header = request.header
        val base = header.base
        val points = ArrayList<TraceMessage.AbsolutePoint>(request.pointsCount)

        // 用 Long 累加：脏数据里的极端增量（Int.MIN_VALUE 等）在 Int 上会静默回绕，
        // 回绕后的坐标反而可能“看起来合法”，落到库里就是一条穿地球的轨迹。
        // 协议侧坐标与增量都是 sint64（02 §2），这里天然按 Long 走，不需要拆箱
        var tsMs = base.tsMs
        var latQ = base.latQ
        var lngQ = base.lngQ
        var firstNegativeDeltaIdx: Int? = null

        request.pointsList.forEachIndexed { idx, delta ->
            if (delta.dTs < 0 && firstNegativeDeltaIdx == null) firstNegativeDeltaIdx = idx
            tsMs += delta.dTs
            latQ += delta.dLat
            lngQ += delta.dLng
            points +=
                TraceMessage.AbsolutePoint
                    .newBuilder()
                    .setTsMs(tsMs)
                    .setLatQ(latQ.coerceToInt())
                    .setLngQ(lngQ.coerceToInt())
                    .setAltitudeCm(delta.altitudeCm)
                    .setSpeedMps(delta.speedMps)
                    .setBearingDeg(delta.bearingDeg)
                    .setAccuracyCm(delta.accuracyCm)
                    .setSource(delta.sourceValue)
                    .setFlags(delta.flags)
                    .setIdx(idx)
                    .build()
        }
        return DecodedBatch(header = header, points = points, firstNegativeDeltaIdx = firstNegativeDeltaIdx)
    }

    /** 越界值先钳到 int 边界，交给校验器按 `INVALID_COORD` 拒收（而不是回绕成假合法坐标）。 */
    private fun Long.coerceToInt(): Int = coerceIn(Int.MIN_VALUE.toLong(), Int.MAX_VALUE.toLong()).toInt()
}

/**
 * 解码结果。
 *
 * [points] 是**未折叠**的全量点列（校验与点数上限都按它算），
 * 落 Kafka 与回给端侧的 `accepted` 用 [foldedPoints]（06 §2.2 第 6 步）。
 */
data class DecodedBatch(
    val header: TraceBatchHeader,
    val points: List<TraceMessage.AbsolutePoint>,
    val firstNegativeDeltaIdx: Int? = null,
) {
    val rawCount: Int get() = points.size

    /**
     * 批内折叠：合并相邻且 `(ts, lat_q, lng_q)` 完全相同的点（06 §2.2 第 6 步）。
     *
     * 只比相邻点、不做全批去重：
     * - 静止时端侧会连续采样出同一位置，这类点是**相邻**的，折叠收益最大；
     * - 远距离重复（A→B→A）本身是真实轨迹信息，不能删。
     *
     * 保留**首点**及其原始 idx，保证 `pointId` 与未折叠时一致。
     */
    fun foldedPoints(): List<TraceMessage.AbsolutePoint> {
        if (points.size <= 1) return points
        val out = ArrayList<TraceMessage.AbsolutePoint>(points.size)
        var prev: TraceMessage.AbsolutePoint? = null
        for (point in points) {
            val last = prev
            if (last != null && last.tsMs == point.tsMs && last.latQ == point.latQ && last.lngQ == point.lngQ) {
                continue
            }
            out += point
            prev = point
        }
        return out
    }
}
