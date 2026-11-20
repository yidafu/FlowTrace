package dev.yidafu.flowtrace.ingest.service

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import dev.yidafu.flowtrace.shared.model.DeviceStatus
import dev.yidafu.flowtrace.shared.persistence.repository.DeviceRepository
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * 设备状态缓存（06 §2.2 第 2 步 / 04 §5）：**进程内 Caffeine**，TTL 60s。
 *
 * 为什么不用 Redis：设备状态的读频率与上报批数同阶（每批一次），而它只是「DISABLED/SUSPENDED
 * 才需要拦」的弱一致判断。放 Redis 等于给每个上报请求加一次网络往返，换来的新鲜度提升只有
 * 一个 TTL 窗口——06 §2.2 明确要求本地缓存（Caffeine, TTL 60s）。
 *
 * 缓存里只放 **device 表真实存在** 的状态：
 * - 查不到 → 返回 ACTIVE 放行（04 §5 首次合法上报自动注册），但**不写缓存**，
 *   否则 [register] 会把这份「推定 ACTIVE」当成已注册状态而跳过落库；
 * - 空 deviceId → 直接 DISABLED，属于确定性失败，不必回源。
 *
 * 失效：admin 改设备状态后调 [invalidate]（多模块单体里是同一进程，直接调 bean 即可）。
 * 多实例部署时需要 `cfg:devver:{deviceId}` 版本号做跨实例失效（06 §6），
 * 那需要 admin 侧配合写版本键，随 admin 实现一并接入。
 */
@Component
class DeviceVersionService(
    private val deviceRepository: DeviceRepository,
) {
    private val cache: Cache<String, DeviceStatus> =
        Caffeine
            .newBuilder()
            // 活跃设备数上限：超出后按 LRU 淘汰，淘汰只是多一次回源，不影响正确性
            .maximumSize(MAX_CACHED_DEVICES)
            .expireAfterWrite(CACHE_TTL)
            .build()

    /** 只读检查：命中缓存直接返回；未命中回源 device 表；查不到按未注册 ACTIVE 放行且不缓存。 */
    fun getStatus(deviceId: String): DeviceStatus {
        if (deviceId.isBlank()) return DeviceStatus.DISABLED

        cache.getIfPresent(deviceId)?.let { return it }
        // 查不到：按未注册放行，不写缓存（留给 register 落库并区分「真实查到的 ACTIVE」）
        val status = deviceRepository.findStatus(deviceId) ?: return DeviceStatus.ACTIVE
        cache.put(deviceId, status)
        return status
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
        cache.getIfPresent(deviceId)?.let { return it }

        val status =
            deviceRepository.findStatus(deviceId) ?: deviceRepository.registerIfAbsent(
                deviceId,
                userId,
                platform = platform,
                appVersion = appVersion,
                sdkVersion = sdkVersion,
            )
        cache.put(deviceId, status)
        return status
    }

    /** 主动失效（admin 改状态后调用）：本进程立即生效，其他实例等 TTL 过期。 */
    fun invalidate(deviceId: String) {
        cache.invalidate(deviceId)
    }

    private companion object {
        val CACHE_TTL: Duration = Duration.ofSeconds(60)

        /** 本地缓存容量：按单实例活跃设备量级估算，超出后 LRU 淘汰。 */
        const val MAX_CACHED_DEVICES: Long = 100_000
    }
}
