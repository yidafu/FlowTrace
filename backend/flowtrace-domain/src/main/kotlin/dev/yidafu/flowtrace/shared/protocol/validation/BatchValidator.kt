package dev.yidafu.flowtrace.shared.protocol.validation

import dev.yidafu.flowtrace.shared.protocol.delta.DecodedBatch
import dev.yidafu.flowtrace.v1.Channel

/**
 * 上报字段校验的错误码（06 §2.3 规则表的「失败码」列，04 §1.4 错误码全集子集）。
 *
 * 故意不直接复用 `platform/common` 的 `ApiErrorCode`：依赖方向是 `services → libs → platform`，
 * 协议库不应反向依赖 HTTP 层。映射由 upload 控制器完成（[ValidationCode] → HTTP 状态 + 错误体）。
 */
enum class ValidationCode {
    INVALID_SCHEMA_VERSION,
    PROTOCOL_ERROR,
    PAYLOAD_TOO_LARGE,
    TS_NOT_SORTED,
    INVALID_COORD,
    TS_OUT_OF_WINDOW,
}

/** 校验失败：[idx] 为出问题的点下标（批级规则为 null），供 04 §2 的 `details.rejectedPoints` 使用。 */
data class ValidationFailure(
    val code: ValidationCode,
    val message: String,
    val idx: Int? = null,
)

/** 校验阈值（默认值即 06 §2.3 口径，可被 upload 配置覆盖）。 */
data class ValidationConfig(
    /** 实时通道单批点数上限（06 §2.3：REALTIME ≤ 100）。 */
    val maxPointsRealtime: Int = 100,
    /** 补传通道单批点数上限（06 §2.3：BACKFILL ≤ 500）。 */
    val maxPointsBackfill: Int = 500,
    /** 实时通道点 ts 允许的最早偏移（48h）。 */
    val tsWindowRealtimePastMs: Long = 172_800_000L,
    /** 补传通道点 ts 允许的最早偏移（7d）。 */
    val tsWindowBackfillPastMs: Long = 604_800_000L,
    /** 未来时间容忍（10min）：端侧时钟漂移不该整批拒收。 */
    val tsWindowFutureMs: Long = 600_000L,
)

/**
 * 上报字段校验器（06 §2.3 规则表，可被 upload 复用）。
 *
 * 为什么放在协议库而不是 controller：规则是**协议的一部分**（02 §9 明确 SDK 侧也要按同一张表
 * 做端侧预检），放在 libs 才能被服务端与将来的校验工具复用；而且它是纯函数，测试不需要 Spring。
 *
 * 规则顺序与文档一致（schema → 身份 → 通道 → 点数 → 排序 → 坐标 → 窗口）：
 * 前面的规则更「便宜且更根本」，命中即返回，避免为一批明显非法的数据跑完整点循环。
 *
 * 不在本类处理的规则：
 * - `batch_id` 合法 UUID：控制器已前置校验（它同时是幂等键，必须先于一切副作用）；
 * - 设备/用户与网关头一致性（`FORBIDDEN`）：需要 HTTP 头，属控制器职责（06 §2.2 第 1 步）；
 * - `speed_mps` / `accuracy_cm` 越界：06 §2.3 明确「不拒收」，交 processor 清洗。
 */
class BatchValidator {
    /**
     * @param nowMs 服务端当前时间（epoch ms）；由调用方传入，便于测试固定时钟
     * @return null 表示通过；否则为第一处失败
     */
    fun validate(
        decoded: DecodedBatch,
        nowMs: Long,
        config: ValidationConfig = ValidationConfig(),
    ): ValidationFailure? {
        val header = decoded.header

        if (!isSupportedSchemaVersion(header.schemaVersion)) {
            return ValidationFailure(
                ValidationCode.INVALID_SCHEMA_VERSION,
                "不支持的 schema_version：'${header.schemaVersion}'（只接受 1.x）",
            )
        }
        if (header.deviceId.isBlank() || header.userId.isBlank()) {
            return ValidationFailure(ValidationCode.PROTOCOL_ERROR, "device_id / user_id 不能为空")
        }

        val channel = header.channel
        if (channel != Channel.CHANNEL_REALTIME && channel != Channel.CHANNEL_BACKFILL) {
            return ValidationFailure(
                ValidationCode.PROTOCOL_ERROR,
                "channel 必须是 1（实时）或 2（补传），实际=${header.channelValue}",
            )
        }
        val backfill = channel == Channel.CHANNEL_BACKFILL

        val limit = if (backfill) config.maxPointsBackfill else config.maxPointsRealtime
        if (decoded.rawCount > limit) {
            return ValidationFailure(
                ValidationCode.PAYLOAD_TOO_LARGE,
                "单批点数超限：channel=${header.channelValue} points=${decoded.rawCount} limit=$limit",
            )
        }

        decoded.firstNegativeDeltaIdx?.let { idx ->
            // 批内 d_ts 必须 ≥ 0：负增量意味着点列时间倒退，后续 ts 窗口/水位/分段全部失准
            return ValidationFailure(ValidationCode.TS_NOT_SORTED, "第 $idx 个点的 d_ts 为负", idx)
        }

        val pastWindowMs = if (backfill) config.tsWindowBackfillPastMs else config.tsWindowRealtimePastMs
        val lowerBound = nowMs - pastWindowMs
        val upperBound = nowMs + config.tsWindowFutureMs

        for (point in decoded.points) {
            // 逐个比较而不走 abs()：abs(Int.MIN_VALUE) 溢出后仍是负数，极端负坐标会漏过检查
            if (point.latQ < -MAX_LAT_Q ||
                point.latQ > MAX_LAT_Q ||
                point.lngQ < -MAX_LNG_Q ||
                point.lngQ > MAX_LNG_Q
            ) {
                return ValidationFailure(
                    ValidationCode.INVALID_COORD,
                    "坐标越界：latQ=${point.latQ} lngQ=${point.lngQ}",
                    point.idx,
                )
            }
            if (point.tsMs < lowerBound || point.tsMs > upperBound) {
                return ValidationFailure(
                    ValidationCode.TS_OUT_OF_WINDOW,
                    "点时间超出允许窗口：tsMs=${point.tsMs} 允许 [$lowerBound, $upperBound]",
                    point.idx,
                )
            }
        }
        return null
    }

    /** schema `1.x`：`1` / `1.0` / `1.0.3` 放行，`2.0` / `0.9` / 空串拒收（02 §3 主版本不兼容）。 */
    private fun isSupportedSchemaVersion(version: String): Boolean =
        version == SUPPORTED_SCHEMA_MAJOR || version.startsWith("$SUPPORTED_SCHEMA_MAJOR.")

    private companion object {
        const val SUPPORTED_SCHEMA_MAJOR = "1"

        /** 纬度量化上限 ±90° × 1e7。 */
        const val MAX_LAT_Q = 900_000_000

        /** 经度量化上限 ±180° × 1e7。 */
        const val MAX_LNG_Q = 1_800_000_000
    }
}
