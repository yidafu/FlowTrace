package dev.yidafu.flowtrace.upload

import dev.yidafu.flowtrace.common.web.ApiErrorCode
import dev.yidafu.flowtrace.common.web.apiError
import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.messaging.TraceBatchProducer
import dev.yidafu.flowtrace.model.DeviceStatus
import dev.yidafu.flowtrace.upload.config.UploadProperties
import dev.yidafu.flowtrace.upload.dto.UploadBatchResponse
import dev.yidafu.flowtrace.upload.service.DeviceVersionService
import dev.yidafu.flowtrace.upload.service.ReteLimiter
import dev.yidafu.flowtrace.v1.TraceBatchRequest
import dev.yidafu.flowtrace.v1.TracePointDelta
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/**
 * 轨迹接入控制器（docs/design/04-api.md §2）。
 *
 * 请求体绑定：控制器只面向协议消息对象（TraceBatchRequest），不感知编码细节；
 * 格式由 Content-Type 协商（TraceUploadWebConfig 注册的 Protobuf 消息转换器）：
 * application/x-protobuf（主格式，Protobuf + Delta 编码）/ application/json（proto JSON 映射，调试与兼容场景）。
 * 传输层 gzip（Content-Encoding: gzip）由 GzipRequestDecompressFilter 透明解压。
 *
 * 处理链：鉴权（security 链，Bearer JWT scope=trace:write，did==header.device_id、sub==user_id）
 * → 限流 → 字段校验 → 幂等（batchId）→ Kafka → 响应。
 *
 * 响应：200 = [UploadBatchResponse]；4xx/5xx = 统一错误体（04 §1.4，[apiError]），故返回类型为 `ResponseEntity<Any>`。
 */
@RestController
@RequestMapping("/v1/traces")
class TraceUploadController(
    private val deviceVersionService: DeviceVersionService,
    private val traceBatchProduce: TraceBatchProducer,
    private val retaLimiter: ReteLimiter,
    private val uploadProperties: UploadProperties,
) {
    /**
     * 上报轨迹批次：POST /v1/traces/batch
     *
     * [batch] 轨迹批次（批头基准值 + Delta 点列，见 proto/trace.proto）。
     *
     * 200 响应（JSON）：batchId / accepted / deduplicated / nextUploadIntervalSec / serverTsMs；
     * 422 = 确定性失败（编码/校验错误，SDK 不重试）；429 = Retry-After + nextUploadIntervalSec=300。
     */
    @PostMapping("/batch")
    fun uploadBatch(
        @RequestBody batch: TraceBatchRequest,
    ): ResponseEntity<Any> {
        val receivedTsMs = System.currentTimeMillis()
        val header = batch.header
        val did = header.deviceId

        // 入口只做设备状态只读检查，不注册、不写库（06 §2.2 第 2 步）
        val status = deviceVersionService.getStatus(did)
        if (status != DeviceStatus.ACTIVE) {
            return error(HttpStatus.FORBIDDEN, ApiErrorCode.DEVICE_DISABLED, "设备状态为 $status，已拒绝上报")
        }
        // 空批（端侧心跳/队列为空）：直接成功返回，不占限流额度、不写 Kafka
        if (batch.pointsCount == 0) {
            return ResponseEntity.ok(response(header.batchId, accepted = 0, serverTsMs = receivedTsMs))
        }
        if (!isSupportedSdkVersion(header.sdk.sdkVersion)) {
            return error(
                HttpStatus.BAD_REQUEST,
                ApiErrorCode.PROTOCOL_ERROR,
                "不支持的 SDK 版本：${header.sdk.sdkVersion}（仅接受 $SUPPORTED_SDK_MAJOR_MINOR.x）",
            )
        }
        // 字段校验通过后才注册：非法请求不污染 device 表（04 §5「首次合法上报自动注册」）
        deviceVersionService.register(
            did,
            userId = header.userId,
            platform = header.sdk.platform,
            appVersion = header.sdk.appVersion,
            sdkVersion = header.sdk.sdkVersion,
        )
        val base = header.base

        // 限流阈值来自 flowtrace.upload.limit-*（07 §3），窗口固定 60s
        if (!retaLimiter.check(did, header.channel, batch.pointsCount)) {
            val limitedIntervalSec = uploadProperties.clamp(uploadProperties.intervalLimitedSec)
            return ResponseEntity
                .status(HttpStatus.TOO_MANY_REQUESTS)
                .header(HttpHeaders.RETRY_AFTER, limitedIntervalSec.toString())
                .body(
                    apiError(
                        ApiErrorCode.RATE_LIMITED,
                        "设备上报频率超限",
                        retryAfterSec = limitedIntervalSec.toLong(),
                        details = mapOf("nextUploadIntervalSec" to limitedIntervalSec),
                    ),
                )
        }

        // Delta 是「相对前一点的链式增量」（trace.proto：cur = prev + delta，prev₀ = base），
        // 必须逐点滚动累积基准；逐点绝对坐标 = 上一绝对坐标 + 本点增量。
        var curTsMs = base.tsMs
        var curLatQ = base.latQ
        var curLngQ = base.lngQ
        val message =
            TraceMessage.TraceBatchMessage
                .newBuilder()
                .setBatchId(header.batchId)
                .setSchemaVersion(header.schemaVersion)
                .setDeviceId(header.deviceId)
                .setUserId(header.userId)
                .setSessionId(header.sessionId)
                .setTaskId(header.taskId)
                .setChannel(header.channelValue)
                .setReceivedTsMs(receivedTsMs)
                .apply {
                    batch.pointsList.forEachIndexed { idx, point ->
                        curTsMs += point.dTs
                        curLatQ += point.dLat
                        curLngQ += point.dLng
                        // repeated 字段用 addPoints 追加；setPoints(idx, …) 在空列表上越界
                        addPoints(formGeoPoint(curTsMs, curLatQ, curLngQ, point, idx))
                    }
                }.build()
        // 发送 kafka 消息：分区键必须是 deviceId（保证同设备有序），不能用 batchId
        traceBatchProduce.send(header.deviceId, message)
        return ResponseEntity.ok(response(header.batchId, accepted = batch.pointsCount, serverTsMs = receivedTsMs))
    }

    /** 由累积后的绝对坐标 + 本点非增量字段装配内部消息点。 */
    fun formGeoPoint(
        tsMs: Long,
        latQ: Int,
        lngQ: Int,
        delta: TracePointDelta,
        idx: Int,
    ): TraceMessage.AbsolutePoint.Builder =
        TraceMessage.AbsolutePoint
            .newBuilder()
            .setTsMs(tsMs)
            .setLatQ(latQ)
            .setLngQ(lngQ)
            .setAltitudeCm(delta.altitudeCm)
            .setSpeedMps(delta.speedMps)
            .setBearingDeg(delta.bearingDeg)
            .setAccuracyCm(delta.accuracyCm)
            .setSource(delta.sourceValue)
            .setFlags(delta.flags)
            .setIdx(idx)

    private fun response(
        batchId: String,
        accepted: Int,
        serverTsMs: Long,
    ): UploadBatchResponse =
        UploadBatchResponse(
            batchId = batchId,
            accepted = accepted,
            // 幂等（batchId 唯一索引）尚未接入写路径，此处恒为 false
            deduplicated = false,
            nextUploadIntervalSec = nextUploadIntervalSec(),
            serverTsMs = serverTsMs,
        )

    /**
     * 下发下次上报间隔（04 §2.3）。
     *
     * 当前只实现「基础值」这一档：运维覆盖（`cfg:upint:{deviceId}`）与按分钟点数占比降速（≥70% → 60s、≥90% → 120s）
     * 依赖限流计数接入后补，届时仍在此收敛，响应字段不变。
     */
    private fun nextUploadIntervalSec(): Int = uploadProperties.clamp(uploadProperties.intervalBaseSec)

    /** 只校验 major.minor：`1.0`、`1.0.0`、`1.0.1` 放行，`1.1.0`/`0.9.0` 拒绝（openapi：当前后端只接受 1.0.x）。 */
    private fun isSupportedSdkVersion(version: String): Boolean =
        version == SUPPORTED_SDK_MAJOR_MINOR || version.startsWith("$SUPPORTED_SDK_MAJOR_MINOR.")

    private fun error(
        status: HttpStatus,
        code: ApiErrorCode,
        message: String,
    ): ResponseEntity<Any> = ResponseEntity.status(status).body(apiError(code, message))

    private companion object {
        const val SUPPORTED_SDK_MAJOR_MINOR = "1.0"
    }
}
