package dev.yidafu.flowtrace.processor.segment

import dev.yidafu.flowtrace.model.GeoMath
import dev.yidafu.flowtrace.model.PointFlags
import dev.yidafu.flowtrace.model.TracePoint
import java.util.UUID

/** 分段算法单测用的点构造器；坐标用「赤道米 → 量化整数」避免手算经纬度。 */
internal object TestPoints {
    private val BATCH_ID: UUID = UUID.fromString("018f2b3c-0000-7000-8000-000000000001")

    fun of(
        tsMs: Long,
        latQ: Int = 0,
        lngQ: Int = 0,
        speedMps: Float? = null,
        flags: Int = 0,
    ): TracePoint =
        TracePoint(
            tsMs = tsMs,
            pointId = UUID(0, tsMs),
            userId = "u-1",
            deviceId = "d-1",
            batchId = BATCH_ID,
            latQ = latQ,
            lngQ = lngQ,
            speedMps = speedMps,
            flags = flags,
        )

    /** 赤道 1° 经度约 111320m，测试只关心相对距离。 */
    fun lngQOfMeters(meters: Double): Int = GeoMath.quantize(meters / 111_320.0)

    fun motionChange(tsMs: Long): Int = PointFlags.MOTION_CHANGE
}
