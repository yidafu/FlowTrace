package dev.yidafu.flowtrace.processor.segment

import dev.yidafu.flowtrace.model.MoveMode
import dev.yidafu.flowtrace.model.TracePoint
import org.springframework.stereotype.Component

/**
 * 运动模式推断（06 §3.4）。
 *
 * 协议里的 `MOTION_CHANGE` 只表示端侧已确认「模式发生变化」，并不携带新模式枚举；
 * 因此服务端仍要从点速度推断具体模式，flag 用于分段时优先采信这个切换点。
 *
 * 阈值集中在本类，不写进 [MoveMode] 枚举——模型层只承载数值口径，避免形成第二份事实源。
 */
@Component
class MoveModeClassifier {
    /**
     * 速度 → 模式。缺省（null）、负数、NaN/Infinity 一律归 [MoveMode.UNKNOWN]。
     *
     * 边界按 06 §3.4：<0.5 STILL，0.5~3 WALK，3~8 RUN，8~15 BICYCLE，≥15 VEHICLE。
     */
    fun fromSpeed(speedMps: Float): MoveMode {
        if (!speedMps.isFinite() || speedMps < 0f) return MoveMode.UNKNOWN
        return when {
            speedMps < LOW_MAX_MPS -> MoveMode.STILL
            speedMps < WALK_MAX_MPS -> MoveMode.WALK
            speedMps < RUN_MAX_MPS -> MoveMode.RUN
            speedMps < BICYCLE_MAX_MPS -> MoveMode.BICYCLE
            else -> MoveMode.VEHICLE
        }
    }

    /** 单点推断：协议 0 = 缺省，清洗后 null 即未知。 */
    fun infer(point: TracePoint): MoveMode = point.speedMps?.let { fromSpeed(it) } ?: MoveMode.UNKNOWN

    /** 单点确认模式；没有可推断速度时归 UNKNOWN。 */
    fun confirmedMode(point: TracePoint): MoveMode = confirmedMode(point, MoveMode.UNKNOWN)

    /**
     * 分段用的确认模式：有点级速度就采信推断值，否则沿用当前窗口模式 [fallback]。
     *
     * 协议没有把模式枚举透传给服务端，`MOTION_CHANGE` 只说明「发生了切换」；
     * 该点速度为缺省时无法推断新模式，保持旧模式比猜一个值更安全，
     * 也不会触发一次无意义的拆段。
     */
    fun confirmedMode(
        point: TracePoint,
        fallback: MoveMode,
    ): MoveMode {
        val inferred = infer(point)
        return if (inferred == MoveMode.UNKNOWN) fallback else inferred
    }

    /**
     * 区间多数投票。UNKNOWN 不参与投票；平票时按输入中首次达到该票数的模式返回，
     * 保证同一批点重算得到完全相同的结果。
     */
    fun majority(points: List<TracePoint>): MoveMode {
        val counts = LinkedHashMap<MoveMode, Int>()
        for (point in points) {
            val mode = infer(point)
            if (mode == MoveMode.UNKNOWN) continue
            counts[mode] = (counts[mode] ?: 0) + 1
        }
        var best = MoveMode.UNKNOWN
        var bestCount = 0
        for ((mode, count) in counts) {
            if (count > bestCount) {
                best = mode
                bestCount = count
            }
        }
        return best
    }

    companion object {
        private const val LOW_MAX_MPS = 0.5f
        private const val WALK_MAX_MPS = 3f
        private const val RUN_MAX_MPS = 8f
        private const val BICYCLE_MAX_MPS = 15f
    }
}
