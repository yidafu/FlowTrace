package dev.yidafu.flowtrace.upload.service

import dev.yidafu.flowtrace.common.cache.FlowtraceCache
import dev.yidafu.flowtrace.model.DeviceStatus
import dev.yidafu.flowtrace.storage.postgres.repository.DeviceRepository
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * 设备状态缓存（06 §2.2 第 2 步 / 04 §5）：只读查询与首次注册分离，缓存只在 Redis 一层。
 *
 * Redis cfg:devstatus:{did} 存状态（TTL 60s，[CACHE_TTL]）：
 * - [getStatus] 只读，供入口状态检查：命中缓存直接返回，未命中回源 device 表；
 * - [register] 幂等注册，仅在字段校验通过后调用，非法请求不写 device 表。
 *
 * 关键：[getStatus] 查不到设备时返回 ACTIVE 放行（04 §5 首次合法上报自动注册），
 * 但**不写缓存**——否则 register 会把这份「推定 ACTIVE」当成已注册状态而跳过落库。
 * 缓存里只放 device 表真实存在的状态，因此 register 命中缓存可安全跳过注册。
 *
 * 缓存 key 用 cfg:devstatus: 而非 cfg:devver:：后者按设计是 admin 写版本号的信号键，
 * 与这里缓存的状态值类型不同，分开避免未来 admin 实现时类型冲突。
 */
@Component
class DeviceVersionService(
    private val cache: FlowtraceCache,
    private val deviceRepository: DeviceRepository,
) {
    /** 只读检查（无任何写操作）：deviceId 空 → DISABLED；DB 查不到 → ACTIVE 放行但不缓存。 */
    fun getStatus(deviceId: String): DeviceStatus {
        if (deviceId.isBlank()) return DeviceStatus.DISABLED

        cache.get(cacheKey(deviceId), DeviceStatus::class.java)?.let { return it }
        // 查不到：按未注册放行，不写缓存（留给 register 落库并区分「真实查到的 ACTIVE」）
        return deviceRepository.findStatus(deviceId) ?: DeviceStatus.ACTIVE
    }

    /**
     * 字段校验通过后的幂等注册：缓存命中说明设备已存在，直接返回；
     * 否则回源查，查不到才 insertIgnore 注册为 ACTIVE，并把真实状态写入缓存。
     */
    fun register(
        deviceId: String,
        userId: String,
        platform: String,
        appVersion: String,
        sdkVersion: String,
    ): DeviceStatus {
        cache.get(cacheKey(deviceId), DeviceStatus::class.java)?.let { return it }

        val status =
            deviceRepository.findStatus(deviceId) ?: deviceRepository.registerIfAbsent(
                deviceId,
                userId,
                platform = platform,
                appVersion = appVersion,
                sdkVersion = sdkVersion,
            )
        cache.put(cacheKey(deviceId), status, CACHE_TTL)
        return status
    }

    private fun cacheKey(deviceId: String) = "$CACHE_KEY_PREFIX$deviceId"

    private companion object {
        const val CACHE_KEY_PREFIX = "cfg:devstatus:"
        val CACHE_TTL: Duration = Duration.ofSeconds(60)
    }
}
