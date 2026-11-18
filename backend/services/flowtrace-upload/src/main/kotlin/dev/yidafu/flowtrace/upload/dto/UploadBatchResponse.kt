package dev.yidafu.flowtrace.upload.dto

/**
 * 上报批次响应（04 §2「200 响应」/ openapi `UploadBatchResponse`）。
 *
 * SDK 侧据此更新本地上报节奏（[nextUploadIntervalSec]）并用 [serverTsMs] 做时钟校正；
 * [deduplicated] 为 true 时表示 batchId 命中幂等、本次未重写 Kafka，SDK 按成功处理。
 */
data class UploadBatchResponse(
    /** 回显的批次 ID（与请求批头一致）。 */
    val batchId: String,
    /** 进入 Kafka 的点数（批内完全重复点折叠后；当前未做批内折叠，等于 `pointsCount`）。 */
    val accepted: Int,
    /** true = batchId 命中幂等、本次未重写 Kafka。 */
    val deduplicated: Boolean,
    /** 服务端下发的下次上报间隔（秒），端侧钳制到 10..300（04 §2.3）。 */
    val nextUploadIntervalSec: Int,
    /** 服务端接收时间（epoch ms），供端侧时钟校正。 */
    val serverTsMs: Long,
)
