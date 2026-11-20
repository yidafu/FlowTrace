package dev.yidafu.flowtrace.ingest

import dev.yidafu.flowtrace.ingest.config.UploadProperties
import dev.yidafu.flowtrace.ingest.dto.UploadBatchResponse
import dev.yidafu.flowtrace.ingest.metrics.UploadMetrics
import dev.yidafu.flowtrace.ingest.service.DeviceVersionService
import dev.yidafu.flowtrace.ingest.service.ReteLimiter
import dev.yidafu.flowtrace.ingest.service.TraceBatchWriter
import dev.yidafu.flowtrace.internal.v1.TraceMessage
import dev.yidafu.flowtrace.shared.cache.IdempotencyGate
import dev.yidafu.flowtrace.shared.messaging.TraceBatchProducer
import dev.yidafu.flowtrace.shared.model.Channel
import dev.yidafu.flowtrace.shared.model.DeviceStatus
import dev.yidafu.flowtrace.shared.model.TraceBatchStatus
import dev.yidafu.flowtrace.shared.persistence.repository.TraceBatchRow
import dev.yidafu.flowtrace.shared.protocol.delta.DeltaBatchDecoder
import dev.yidafu.flowtrace.shared.protocol.validation.BatchValidator
import dev.yidafu.flowtrace.shared.protocol.validation.ValidationCode
import dev.yidafu.flowtrace.shared.protocol.validation.ValidationConfig
import dev.yidafu.flowtrace.shared.protocol.validation.ValidationFailure
import dev.yidafu.flowtrace.shared.web.ApiErrorCode
import dev.yidafu.flowtrace.shared.web.apiError
import dev.yidafu.flowtrace.v1.TraceBatchRequest
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * 轨迹接入控制器（docs/design/04-api.md §2）。
 *
 * 请求体绑定：控制器只面向协议消息对象（TraceBatchRequest），不感知编码细节；
 * 格式由 Content-Type 协商（TraceUploadWebConfig 注册的 Protobuf 消息转换器）：
 * application/x-protobuf（主格式，Protobuf + Delta 编码）/ application/json（proto JSON 映射，调试与兼容场景）。
 * 传输层 gzip（Content-Encoding: gzip）由 GzipRequestDecompressFilter 透明解压。
 *
 * 处理链严格按 06 §2.2：batchId → 设备状态 → 空批 → SDK 版本 → **解码 → 字段校验 → 限流 → 折叠
 * → 幂等 → Kafka → 异步台账 → 响应**。
 *
 * 顺序上有两处容易搞反、且都被 D18 钉死：
 * - **校验在限流之前**：非法请求不该消耗配额（否则一台发脏数据的设备会在 60s 内把自己的
 *   合法上报也一起锁死）；
 * - **幂等在 Kafka 之前**：占位是「本批由我投递」的承诺，先写 Kafka 再占位等于没有幂等。
 *
 * 响应：200 = [UploadBatchResponse]；4xx/5xx = 统一错误体（04 §1.4，[apiError]），故返回类型为 `ResponseEntity<Any>`。
 *
 * 日志断点（07 §5 口径：只带 ID 与计数，**不出坐标**）：
 * - INFO「upload 批次接收完成」：正常路径每批一行，带 batchId/deviceId/accepted/costMs/nextUploadIntervalSec；
 * - DEBUG：入口、解码/折叠结果、设备注册、Kafka 写入耗时、批次记录落库；
 * - WARN/ERROR：batchId 非法、设备非 ACTIVE、SDK 版本不支持、字段校验失败、限流、Kafka 写入失败、台账队列满。
 *
 * 指标出口全部收敛在 [UploadMetrics]：`ft_upload_requests_total{code}` 按返回分支计数（`error()` /
 * `validationError()` / `response()` 三个出口各记一次，新增分支不会漏），耗时 `ft_upload_duration_seconds`
 * 只在 Kafka 确认后的成功批上记一次。
 */
@RestController
@RequestMapping("/v1/traces")
class TraceUploadController(
    private val deviceVersionService: DeviceVersionService,
    private val traceBatchProduce: TraceBatchProducer,
    private val retaLimiter: ReteLimiter,
    private val uploadProperties: UploadProperties,
    private val batchWriter: TraceBatchWriter,
    private val idempotencyGate: IdempotencyGate,
    private val metrics: UploadMetrics,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /** 协议库里的纯函数组件：无状态、无 IO，直接持有实例即可，不必做成 Spring bean。 */
    private val decoder = DeltaBatchDecoder()
    private val validator = BatchValidator()

    /**
     * 上报轨迹批次：POST /v1/traces/batch
     *
     * [batch] 轨迹批次（批头基准值 + Delta 点列，见 proto/trace.proto）。
     *
     * 200 响应（JSON）：batchId / accepted / deduplicated / nextUploadIntervalSec / serverTsMs；
     * 422 = 确定性失败（编码/校验错误，SDK 不重试）；413 = 单批点数超限；
     * 429 = Retry-After + nextUploadIntervalSec=300。
     */
    @PostMapping("/batch")
    fun uploadBatch(
        @RequestBody batch: TraceBatchRequest,
    ): ResponseEntity<Any> {
        val receivedTsMs = System.currentTimeMillis()
        // 单调时钟：耗时要抗 NTP 回拨，不能用 wall clock 相减（receivedTsMs 只用于协议回填与日志）
        val startedNs = System.nanoTime()
        val header = batch.header
        val did = header.deviceId

        // 断点①：入口。访问日志只有 method/uri，这里补上 batchId 才能和 processor 侧串起来。
        // 默认 debug：正常情况下每批只留下面一行 INFO 完成摘要（07 §5「正常请求一行」）
        log.debug(
            "upload 收到批次 batchId={} deviceId={} userId={} channel={} points={}",
            header.batchId,
            did,
            header.userId,
            header.channel,
            batch.pointsCount,
        )

        // batchId 前置校验（确定性失败）：解析不出 UUID 就直接 422，不写 Kafka、不落台账、不占限流额度
        val batchId = parseBatchId(header.batchId)
        if (batchId == null) {
            log.warn("upload 拒绝上报：batchId 不是合法 UUID batchId='{}' deviceId={}", header.batchId, did)
            return error(
                HttpStatus.UNPROCESSABLE_ENTITY,
                ApiErrorCode.PROTOCOL_ERROR,
                "batchId 必须是 UUID：'${header.batchId}'",
            )
        }

        // 入口只做设备状态只读检查，不注册、不写库（06 §2.2 第 2 步）
        val status = deviceVersionService.getStatus(did)
        if (status != DeviceStatus.ACTIVE) {
            log.warn(
                "upload 拒绝上报：设备状态非 ACTIVE batchId={} deviceId={} status={}",
                header.batchId,
                did,
                status,
            )
            return error(HttpStatus.FORBIDDEN, ApiErrorCode.DEVICE_DISABLED, "设备状态为 $status，已拒绝上报")
        }
        // 空批（端侧心跳/队列为空）：直接成功返回，不占限流额度、不写 Kafka
        if (batch.pointsCount == 0) {
            log.debug("upload 空批直接返回 batchId={} deviceId={}", header.batchId, did)
            return ResponseEntity.ok(response(header.batchId, accepted = 0, serverTsMs = receivedTsMs))
        }
        if (!isSupportedSdkVersion(header.sdk.sdkVersion)) {
            log.warn(
                "upload 拒绝上报：SDK 版本不支持 batchId={} deviceId={} sdkVersion={}",
                header.batchId,
                did,
                header.sdk.sdkVersion,
            )
            return error(
                HttpStatus.BAD_REQUEST,
                ApiErrorCode.PROTOCOL_ERROR,
                "不支持的 SDK 版本：${header.sdk.sdkVersion}（仅接受 $SUPPORTED_SDK_MAJOR_MINOR.x）",
            )
        }

        // 解码：Delta 链式增量 → 绝对坐标（06 §1.2 trace-protocol）
        val decoded = decoder.decode(batch)
        // 字段校验（06 §2.3 规则表）：失败即确定性失败，不注册设备、不占配额
        val failure = validator.validate(decoded, nowMs = receivedTsMs, config = validationConfig())
        if (failure != null) {
            log.warn(
                "upload 拒绝上报：字段校验失败 batchId={} deviceId={} channel={} points={} code={} idx={} reason={}",
                header.batchId,
                did,
                header.channelValue,
                batch.pointsCount,
                failure.code,
                failure.idx,
                failure.message,
            )
            return validationError(failure)
        }

        // 字段校验通过后才注册：非法请求不污染 device 表（04 §5「首次合法上报自动注册」）
        val registeredStatus =
            deviceVersionService.register(
                did,
                userId = header.userId,
                platform = header.sdk.platform,
                appVersion = header.sdk.appVersion,
                sdkVersion = header.sdk.sdkVersion,
            )
        log.debug(
            "upload 设备注册/刷新完成 batchId={} deviceId={} userId={} platform={} appVersion={} sdkVersion={} status={}",
            header.batchId,
            did,
            header.userId,
            header.sdk.platform,
            header.sdk.appVersion,
            header.sdk.sdkVersion,
            registeredStatus,
        )

        // 限流（06 §2.2 第 5 步 / D18）：在解码 + 校验之后按通道精确计数，非法请求不消耗配额
        if (!retaLimiter.check(did, header.channel, decoded.rawCount)) {
            val limitedIntervalSec = uploadProperties.clamp(uploadProperties.intervalLimitedSec)
            log.warn(
                "upload 限流拒绝 batchId={} deviceId={} channel={} points={} nextUploadIntervalSec={}",
                header.batchId,
                did,
                header.channel,
                decoded.rawCount,
                limitedIntervalSec,
            )
            // 请求分布记 RATE_LIMITED；「哪个通道被限」由 ReteLimiter 的 ft_ratelimit_hits_total 承担
            metrics.request(ApiErrorCode.RATE_LIMITED)
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

        // 批内折叠（06 §2.2 第 6 步）：相邻完全重复的点合并，accepted 以折叠后为准
        val points = decoded.foldedPoints()
        log.debug(
            "upload 解码与折叠完成 batchId={} deviceId={} raw={} folded={} firstTsMs={} lastTsMs={}",
            header.batchId,
            did,
            decoded.rawCount,
            points.size,
            points.first().tsMs,
            points.last().tsMs,
        )

        // 幂等（06 §2.2 第 7 步 / D19）：SETNX `idem:batch:{batchId}`，命中说明该批已受理过。
        val idemKey = batchId.toString()
        if (!idempotencyGate.tryAcquire(idemKey)) {
            log.info(
                "upload 重复批（幂等命中，未写 Kafka） batchId={} deviceId={} points={}",
                header.batchId,
                did,
                points.size,
            )
            // 重复批单独计数：它和 5xx 一样会让「上报成功率」失真，但根因是端侧重试而非服务故障
            metrics.duplicateBatch()
            return ResponseEntity.ok(
                response(header.batchId, accepted = 0, deduplicated = true, serverTsMs = receivedTsMs),
            )
        }

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
                .addAllPoints(points)
                .build()

        // 发送 kafka 消息：分区键必须是 deviceId（保证同设备有序），不能用 batchId
        val kafkaStartNs = System.nanoTime()
        try {
            traceBatchProduce.send(header.deviceId, message)
        } catch (ex: Exception) {
            // D19：先回滚幂等占位再 5xx。留着占位会让 SDK 的重试被当成重复批静默丢弃，
            // 而 Kafka 里其实没有这批数据——「返回 200 但没写 Kafka」是绝对不允许的状态。
            idempotencyGate.release(idemKey)
            // 指标先记再抛：异常出栈后由 GlobalExceptionHandler 统一回 500，这里补上业务维度
            metrics.kafkaSendFailure()
            metrics.request(ApiErrorCode.INTERNAL)
            log.error(
                "upload 写入 Kafka 失败（回滚幂等占位，本次返回 5xx，端侧按退避重试） batchId={} deviceId={} points={} costMs={} cause={}",
                header.batchId,
                did,
                points.size,
                elapsedSinceNs(kafkaStartNs),
                ex.message,
            )
            throw ex
        }
        log.debug(
            "upload 写入 Kafka 完成 batchId={} deviceId={} partitionKey={} points={} costMs={}",
            header.batchId,
            did,
            did,
            points.size,
            elapsedSinceNs(kafkaStartNs),
        )
        persistBatchRecord(
            TraceBatchRow(
                batchId = batchId,
                userId = header.userId,
                deviceId = header.deviceId,
                channel = Channel.from(header.channelValue.toShort()),
                pointCount = points.size,
                firstTs = Instant.ofEpochMilli(points.first().tsMs),
                lastTs = Instant.ofEpochMilli(points.last().tsMs),
                clientTs = Instant.ofEpochMilli(header.clientTsMs),
                receivedAt = Instant.ofEpochMilli(receivedTsMs),
                sdkVersion = header.sdk.sdkVersion,
                platform = header.sdk.platform,
                status = TraceBatchStatus.ACCEPTED,
                // ACCEPTED 阶段尚未处理：error_code / processed_at 必须留空，
                // 否则台账上「未处理」的批看起来像已处理（processor 的 markProcessed 才是唯一写入点）
                errorCode = null,
                processedAt = null,
            ),
        )
        // 断点②：接收完成（Kafka 已确认 + 批次记录已入队），accepted 即折叠后进入 Kafka 的点数
        log.info(
            "upload 批次接收完成 batchId={} deviceId={} accepted={} raw={} costMs={} nextUploadIntervalSec={}",
            header.batchId,
            did,
            points.size,
            decoded.rawCount,
            elapsedSinceMs(receivedTsMs),
            nextUploadIntervalSec(),
        )
        // 07 §4 `ft_upload_duration_seconds`：只在这条「真正写进 Kafka 的成功批」上记一次，
        // 口径见 UploadMetrics.uploadDuration（快速拒绝不进直方图，否则会拉低 p99）
        metrics.uploadDuration(Duration.ofNanos(System.nanoTime() - startedNs))
        return ResponseEntity.ok(response(header.batchId, accepted = points.size, serverTsMs = receivedTsMs))
    }

    /**
     * 批记录交给协程消费者（06 §2.2 第 9 步）；队列满/停机即丢弃并告警，**不反压主上报链路**——
     * 此时 Kafka 已确认，丢的只是台账一行，事后可按 batchId 重放补齐。
     */
    private fun persistBatchRecord(row: TraceBatchRow) {
        if (!batchWriter.submit(row)) {
            log.error(
                "upload 批记录队列已满或正在停机，丢弃该条台账 batchId={} deviceId={} points={}",
                row.batchId,
                row.deviceId,
                row.pointCount,
            )
        }
    }

    /** batchId 必须能解析成 UUID（`trace_batch` 主键 / Kafka 头 bId 一致）；非法返回 null。 */
    private fun parseBatchId(raw: String): UUID? =
        try {
            UUID.fromString(raw)
        } catch (ex: IllegalArgumentException) {
            null
        }

    private fun elapsedSinceMs(startMs: Long): Long = System.currentTimeMillis() - startMs

    private fun elapsedSinceNs(startNs: Long): Long = (System.nanoTime() - startNs) / NANOS_PER_MILLI

    /** 校验阈值全部来自 `flowtrace.upload.*`（07 §3），默认值即 06 §2.3 口径。 */
    private fun validationConfig(): ValidationConfig =
        ValidationConfig(
            maxPointsRealtime = uploadProperties.maxPointsRt,
            maxPointsBackfill = uploadProperties.maxPointsBf,
            tsWindowRealtimePastMs = uploadProperties.tsWindowRtPastMs,
            tsWindowBackfillPastMs = uploadProperties.tsWindowBfPastMs,
            tsWindowFutureMs = uploadProperties.tsWindowFutureMs,
        )

    /**
     * 校验失败 → HTTP 错误体：只有「点数超限」是 413，其余都是 422（确定性失败，SDK 整批丢弃）。
     *
     * `details.rejectedPoints` 尽力给出首个失败点（02 §9 要求端侧能定位是哪一条）：
     * 整批拒收时逐点列出没有意义，一条足够排查。
     */
    private fun validationError(failure: ValidationFailure): ResponseEntity<Any> {
        val status = if (failure.code == ValidationCode.PAYLOAD_TOO_LARGE) HttpStatus.PAYLOAD_TOO_LARGE else HttpStatus.UNPROCESSABLE_ENTITY
        val code =
            when (failure.code) {
                ValidationCode.INVALID_SCHEMA_VERSION -> ApiErrorCode.INVALID_SCHEMA_VERSION
                ValidationCode.PAYLOAD_TOO_LARGE -> ApiErrorCode.PAYLOAD_TOO_LARGE
                ValidationCode.TS_NOT_SORTED -> ApiErrorCode.TS_NOT_SORTED
                ValidationCode.INVALID_COORD -> ApiErrorCode.INVALID_COORD
                ValidationCode.TS_OUT_OF_WINDOW -> ApiErrorCode.TS_OUT_OF_WINDOW
                ValidationCode.PROTOCOL_ERROR -> ApiErrorCode.PROTOCOL_ERROR
            }
        val details =
            failure.idx?.let {
                mapOf("rejectedPoints" to listOf(mapOf("idx" to it, "reason" to failure.code.name)))
            }
        // 确定性失败（422/413）也要计数：5xx 占比告警看的是 code 分布，漏掉这类会让分母失真
        metrics.request(code)
        return ResponseEntity.status(status).body(apiError(code, failure.message, details = details))
    }

    private fun response(
        batchId: String,
        accepted: Int,
        serverTsMs: Long,
        deduplicated: Boolean = false,
    ): UploadBatchResponse {
        // 所有 200 都从这里出（空批 / 幂等命中 / 正常接收），成功计数只在此处 +1，不会漏也不会双计
        metrics.requestOk()
        return UploadBatchResponse(
            batchId = batchId,
            accepted = accepted,
            deduplicated = deduplicated,
            nextUploadIntervalSec = nextUploadIntervalSec(),
            serverTsMs = serverTsMs,
        )
    }

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

    /**
     * 显式错误出口（4xx/5xx）：**所有走这里的失败都自动计入 `ft_upload_requests_total{code}`**，
     * 新增分支不必再记得埋点。[validationError] 与限流分支自己构造响应体，各自单独计数。
     */
    private fun error(
        status: HttpStatus,
        code: ApiErrorCode,
        message: String,
    ): ResponseEntity<Any> {
        metrics.request(code)
        return ResponseEntity.status(status).body(apiError(code, message))
    }

    private companion object {
        const val SUPPORTED_SDK_MAJOR_MINOR = "1.0"
        const val NANOS_PER_MILLI = 1_000_000L
    }
}
