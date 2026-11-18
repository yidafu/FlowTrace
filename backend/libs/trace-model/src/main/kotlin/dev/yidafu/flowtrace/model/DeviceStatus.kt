package dev.yidafu.flowtrace.model

/**
 * 设备状态（03 §5 device.status，int2）：
 * - ACTIVE：正常；
 * - DEGRADED：连续高负载自动降级，限流阈值减半（06 §2.4）；
 * - SUSPENDED / DISABLED：upload 拒收（403 DEVICE_DISABLED）。
 */
enum class DeviceStatus(
    val value: Short,
) {
    ACTIVE(1),
    DEGRADED(2),
    SUSPENDED(3),
    DISABLED(4),
    ;

    companion object {
        fun from(value: Short): DeviceStatus =
            entries.firstOrNull { it.value == value }
                ?: throw IllegalArgumentException("未知设备状态值: $value")
    }
}
