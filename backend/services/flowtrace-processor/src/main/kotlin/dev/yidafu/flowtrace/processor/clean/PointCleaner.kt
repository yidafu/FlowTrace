package dev.yidafu.flowtrace.processor.clean

import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.model.GeoMath
import dev.yidafu.flowtrace.model.Ids
import dev.yidafu.flowtrace.model.LocationSource
import dev.yidafu.flowtrace.model.PointFlags
import dev.yidafu.flowtrace.model.TracePoint
import dev.yidafu.flowtrace.processor.config.ProcessorProperties
import dev.yidafu.flowtrace.processor.segment.DeviceState
import org.springframework.stereotype.Component
import java.util.EnumMap

/**
 * 点级清洗（06 §3.3 规则表），批内按 ts 升序逐点过滤。
 *
 * 目的：把端侧定位噪声（零岛、漂移、重复、时钟超前）挡在写库之前。
 * `trace_point` 里因此不存在「坏点」，迟到重算读到的也是同一口径的数据。
 *
 * 为什么必须有跨批状态 [DeviceState.lastKept]：速度/精度/完全重复三条规则都要和
 * 「上一个**保留**点」比，而这个点可能在上一批里。被丢弃的点不更新 lastKept——
 * 否则连续漂移会一路把基准拖走。
 */
@Component
class PointCleaner(
    private val props: ProcessorProperties,
) {
    /** 清洗结果：[points] 可写库，[dropped] 按原因计数供指标使用（08 §12）。 */
    data class Result(
        val points: List<TracePoint>,
        val dropped: Map<DropReason, Int>,
    ) {
        fun isNotEmpty(): Boolean = points.isNotEmpty()
    }

    /**
     * [points] 为内部消息的点列（已经 upload 校验，ts 升序）；[batchId] 用原样字符串，
     * 因为 `Ids.pointId` 按字符串派生，解析成 UUID 再 toString 可能因大小写不同而派生错。
     *
     * 注意：签名与原桩不同——构造 [TracePoint] 需要 userId/deviceId（来自 state）与 batchId，
     * 返回 `Boolean` 也表达不了「留下哪些点 + 各丢弃原因计数」。
     */
    fun clean(
        points: List<TraceMessage.AbsolutePoint>,
        state: DeviceState,
        nowMs: Long,
        batchId: String,
    ): Result {
        val kept = ArrayList<TracePoint>(points.size)
        val dropped = EnumMap<DropReason, Int>(DropReason::class.java)
        val futureLimit = nowMs + props.futureMs
        var prev = state.lastKept

        for (p in points) {
            val candidate = p.toTracePoint(batchId, state)
            val reason = dropReason(candidate, prev, futureLimit)

            if (reason != null) {
                dropped[reason] = (dropped[reason] ?: 0) + 1
                continue
            }

            kept += candidate
            // 批间逆序的点可以保留（交给水位/dirty_day 处理），但不能让 lastKept 回退
            if (prev == null || candidate.tsMs >= prev.tsMs) {
                prev = candidate
            }
        }

        state.lastKept = prev
        return Result(kept, dropped)
    }

    /** 按 06 §3.3 顺序判定；返回 null 表示保留。FORCED 关键点跳过全部规则。 */
    private fun dropReason(
        p: TracePoint,
        prev: TracePoint?,
        futureLimit: Long,
    ): DropReason? {
        if (p.hasFlag(PointFlags.FORCED)) return null
        if (p.latQ == 0 && p.lngQ == 0) return DropReason.ZERO
        if (p.tsMs > futureLimit) return DropReason.FUTURE
        if (prev == null) return null

        val sameTs = p.tsMs == prev.tsMs
        if (sameTs && p.latQ == prev.latQ && p.lngQ == prev.lngQ) return DropReason.DUPLICATE

        val dt = p.tsMs - prev.tsMs
        if (dt < 0) return null // 乱序点到，时间类规则无从判断，不在这里丢
        if (dt == 0L) return DropReason.SPEED // 同毫秒却坐标不同：物理上不可能

        val distanceM = GeoMath.haversineM(prev.latQ, prev.lngQ, p.latQ, p.lngQ)
        val impliedSpeedMps = distanceM * 1_000 / dt
        if (impliedSpeedMps > props.cleanSpeedMaxMps) return DropReason.SPEED

        // 精度规则：只丢「短时间内的差精度」，长时间静止/低速的差精度点仍保留
        val accuracy = p.accuracyCm?.takeIf { it != 0 }
        if (accuracy != null && accuracy > props.cleanAccuracyCm && dt < props.cleanAccuracyLoneMs) {
            return DropReason.ACCURACY
        }
        return null
    }

    /** proto 点 → 领域点；协议里「0 = 缺省」的可选字段在此归一为 null。 */
    private fun TraceMessage.AbsolutePoint.toTracePoint(
        batchId: String,
        state: DeviceState,
    ): TracePoint {
        val userId = state.userId.orEmpty()
        val deviceId = state.deviceId
        return TracePoint(
            tsMs = tsMs,
            pointId = Ids.pointId(batchId, idx),
            userId = userId,
            deviceId = deviceId,
            batchId = java.util.UUID.fromString(batchId),
            latQ = latQ,
            lngQ = lngQ,
            sessionId = state.sessionId,
            taskId = state.taskId,
            accuracyCm = accuracyCm.takeIf { it != 0 },
            speedMps = speedMps.takeIf { it != 0f },
            bearingDeg = bearingDeg.toShort().takeIf { it.toInt() != 0 },
            altitudeCm = altitudeCm.takeIf { it != 0 },
            source = LocationSource.fromProto(source),
            flags = flags,
        )
    }
}
