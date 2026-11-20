package dev.yidafu.flowtrace.ingest.service

import dev.yidafu.flowtrace.shared.model.DeviceStatus
import dev.yidafu.flowtrace.shared.persistence.repository.DeviceRepository
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * 设备状态缓存测试（04 §5 / 06 §2.2 第 2 步）。
 *
 * 这里守的是四条容易写反的语义：
 * - 未注册设备按 ACTIVE 放行，但**不缓存**——否则 `register` 会把这份「推定 ACTIVE」
 *   当成已注册状态而跳过落库，设备永远进不了 device 表；
 * - 库中真实存在的状态才进缓存，因此 `register` 命中缓存可安全短路；
 * - `deviceId` 为空是确定性失败（DISABLED），不必回源；
 * - `invalidate` 之后必须重新回源（admin 改状态的最小可用失效路径）。
 *
 * 缓存是纯内存 Caffeine，断言全部落在「回源次数」上，不依赖 Redis 或时间流逝。
 */
class DeviceVersionServiceTest {
    private val repository = RecordingDeviceRepository()
    private val service = DeviceVersionService(repository)

    @Test
    fun `deviceId 为空直接判 DISABLED 且不查库`() {
        assertEquals(DeviceStatus.DISABLED, service.getStatus(""))

        assertEquals(0, repository.findStatusCalls, "空 deviceId 不该回源 device 表")
    }

    @Test
    fun `首次回源之后命中缓存不再查库`() {
        repository.status = DeviceStatus.SUSPENDED

        assertEquals(DeviceStatus.SUSPENDED, service.getStatus(DEVICE_ID))
        assertEquals(DeviceStatus.SUSPENDED, service.getStatus(DEVICE_ID))

        assertEquals(1, repository.findStatusCalls, "第二次应命中本地缓存")
    }

    @Test
    fun `库中无记录按 ACTIVE 放行且不缓存负结果`() {
        repository.status = null

        assertEquals(DeviceStatus.ACTIVE, service.getStatus(DEVICE_ID))
        assertEquals(DeviceStatus.ACTIVE, service.getStatus(DEVICE_ID))

        assertEquals(2, repository.findStatusCalls, "推定 ACTIVE 不能缓存，否则 register 会跳过落库")
    }

    @Test
    fun `库中存在记录时以其状态为准`() {
        repository.status = DeviceStatus.DISABLED

        assertEquals(DeviceStatus.DISABLED, service.getStatus(DEVICE_ID))
    }

    @Test
    fun `register 命中缓存直接返回且不写库`() {
        repository.status = DeviceStatus.DISABLED
        service.getStatus(DEVICE_ID) // 预热缓存

        val status = service.register(DEVICE_ID, USER_ID, PLATFORM, APP_VERSION, SDK_VERSION)

        assertEquals(DeviceStatus.DISABLED, status)
        assertEquals(0, repository.registerCalls)
        assertEquals(1, repository.findStatusCalls, "register 不该再回源一次")
    }

    @Test
    fun `register 首次注册写库并缓存`() {
        repository.status = null

        val status = service.register(DEVICE_ID, USER_ID, PLATFORM, APP_VERSION, SDK_VERSION)

        assertEquals(DeviceStatus.ACTIVE, status)
        assertEquals(1, repository.registerCalls)
        // 缓存已写：紧接着的 getStatus 不应再回源
        service.getStatus(DEVICE_ID)
        assertEquals(1, repository.findStatusCalls)
    }

    @Test
    fun `register 对已存在设备回读真实状态不用 insert`() {
        repository.status = DeviceStatus.SUSPENDED

        val status = service.register(DEVICE_ID, USER_ID, PLATFORM, APP_VERSION, SDK_VERSION)

        assertEquals(DeviceStatus.SUSPENDED, status)
        assertEquals(0, repository.registerCalls, "已存在设备不应再走 insertIgnore")
    }

    @Test
    fun `invalidate 之后重新回源`() {
        repository.status = DeviceStatus.ACTIVE
        service.getStatus(DEVICE_ID)

        repository.status = DeviceStatus.DISABLED
        service.invalidate(DEVICE_ID)

        assertEquals(DeviceStatus.DISABLED, service.getStatus(DEVICE_ID))
        assertEquals(2, repository.findStatusCalls)
    }

    private companion object {
        const val DEVICE_ID = "dev-1"
        const val USER_ID = "u-1"
        const val PLATFORM = "android"
        const val APP_VERSION = "1.2.3"
        const val SDK_VERSION = "1.0.0"
    }

    /** 记录调用次数的手写替身：本模块只有 upload 侧读路径，不需要真实 SQL。 */
    private class RecordingDeviceRepository : DeviceRepository() {
        var status: DeviceStatus? = null
        var findStatusCalls = 0
        var registerCalls = 0

        override fun findStatus(deviceId: String): DeviceStatus? {
            findStatusCalls++
            return status
        }

        override fun registerIfAbsent(
            deviceId: String,
            userId: String,
            platform: String?,
            appVersion: String?,
            sdkVersion: String?,
        ): DeviceStatus {
            registerCalls++
            return status ?: DeviceStatus.ACTIVE
        }
    }
}
