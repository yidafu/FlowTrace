package dev.yidafu.flowtrace.shared.model

import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.UUID

/**
 * 轨迹点（03 §2 `trace_point` 的 1:1 内存映射，全 int 量化）。
 *
 * 约定：
 * - 坐标只有量化整数 [latQ] / [lngQ]（度 × 10⁷），**链路上不传 double**，
 *   避免「服务端二次量化」与端侧量化结果不一致；
 * - [pointId] 由 `UUIDv5(FLOWTRACE_POINT_NS, "{batchId}:{idx}")` 派生（02 §7），
 *   是点级幂等键，落库走 `ON CONFLICT DO NOTHING`；
 * - 协议中「0 = 缺省」的可选字段（精度/速度/航向/海拔/来源）在这里是 `null`，
 *   由映射层在 0 与 null 之间转换——真实测得的 0 也会被当作缺省，这是协议既定口径。
 */
data class TracePoint(
    val tsMs: Long,
    val pointId: UUID,
    val userId: String,
    val deviceId: String,
    val batchId: UUID,
    val latQ: Int,
    val lngQ: Int,
    val sessionId: String? = null,
    val taskId: String? = null,
    val accuracyCm: Int? = null,
    val speedMps: Float? = null,
    val bearingDeg: Short? = null,
    val altitudeCm: Int? = null,
    val source: LocationSource? = null,
    val flags: Int = 0,
) {
    /** 是否带某个标志位；未知位不影响判定（用 `and` 而非等值比较）。 */
    fun hasFlag(mask: Int): Boolean = flags and mask != 0

    /** 运动状态切换点：processor 分段时优先采信端侧判定（06 §3.4）。 */
    val hasMotionChange: Boolean
        get() = hasFlag(PointFlags.MOTION_CHANGE)

    /**
     * 点所在 UTC 日（`dirty_day.day` 与分区边界同口径，03 §7）。
     *
     * 必须用 UTC：分区按 UTC 日切分，用本地时区会让「迟到判定命中的日期」与
     * 「实际所在分区」错位，重算范围漏日。
     */
    fun utcDay(): LocalDate = Instant.ofEpochMilli(tsMs).atZone(ZoneOffset.UTC).toLocalDate()
}
