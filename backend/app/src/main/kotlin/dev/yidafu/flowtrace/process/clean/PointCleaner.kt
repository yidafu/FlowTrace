package dev.yidafu.flowtrace.process.clean

import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.process.config.ProcessorProperties
import dev.yidafu.flowtrace.process.pipeline.BatchMeta
import dev.yidafu.flowtrace.process.segment.DeviceState
import dev.yidafu.flowtrace.shared.model.GeoMath
import dev.yidafu.flowtrace.shared.model.Ids
import dev.yidafu.flowtrace.shared.model.LocationSource
import dev.yidafu.flowtrace.shared.model.PointFlags
import dev.yidafu.flowtrace.shared.model.TracePoint
import org.springframework.stereotype.Component
import java.util.EnumMap

/**
 * 点级清洗（06 §3.3 规则表），批内按 ts 升序逐点过滤。
 *
 * 目的：把端侧定位噪声（零岛、漂移、重复、时钟超前）挡在写库之前。
 * `trace_point` 里因此不存在「坏点」，迟到重算读到的也是同一口径的数据。
 *
 * 产出的 [TracePoint] 就是待写库的行（身份列取自批头 [BatchMeta]，1:1 映射），
 * 调用方不再需要二次覆盖身份——`DeviceState` 只是清洗的跨批基准，不是身份来源。
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
     * @param points 内部消息的点列（已经 upload 校验，ts 升序）
     * @param meta 本批批头：身份列与批号（批号的**原样字符串**用于派生 pointId）
     * @param state 设备跨批状态：只读 [DeviceState.lastKept]，写回新的保留点
     * @param nowMs 服务端当前时刻，「未来时间」规则的基准
     */
    fun clean(
        points: List<TraceMessage.AbsolutePoint>,
        meta: BatchMeta,
        state: DeviceState,
        nowMs: Long,
    ): Result {
        val kept = ArrayList<TracePoint>(points.size)
        val dropped = EnumMap<DropReason, Int>(DropReason::class.java)
        val futureLimit = nowMs + props.futureMs
        // proto3 string 缺省为空串，库列 nullable：空串统一收敛为 null，查询侧不写两套判空
        val sessionId = meta.sessionId.ifEmpty { null }
        val taskId = meta.taskId.ifEmpty { null }
        var prev = state.lastKept

        for (p in points) {
            val candidate = p.toTracePoint(meta, sessionId, taskId)
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

    /** proto 点 → 领域点（即库行）；协议里「0 = 缺省」的可选字段在此归一为 null。 */
    private fun TraceMessage.AbsolutePoint.toTracePoint(
        meta: BatchMeta,
        sessionId: String?,
        taskId: String?,
    ): TracePoint =
        TracePoint(
            tsMs = tsMs,
            // 批号按原样字符串派生；meta.batchId 已解析好，这里不再逐点 fromString
            pointId = Ids.pointId(meta.batchIdText, idx),
            userId = meta.userId,
            deviceId = meta.deviceId,
            batchId = meta.batchId,
            latQ = latQ,
            lngQ = lngQ,
            sessionId = sessionId,
            taskId = taskId,
            accuracyCm = accuracyCm.takeIf { it != 0 },
            speedMps = speedMps.takeIf { it != 0f },
            bearingDeg = bearingDeg.toShort().takeIf { it.toInt() != 0 },
            altitudeCm = altitudeCm.takeIf { it != 0 },
            source = LocationSource.fromProto(source),
            flags = flags,
        )
}
