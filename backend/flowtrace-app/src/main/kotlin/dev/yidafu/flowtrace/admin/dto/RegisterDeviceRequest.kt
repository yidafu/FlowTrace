package dev.yidafu.flowtrace.admin.dto

/** 预注册设备请求体（POST /v1/admin/devices，可选流程；未注册设备首次合法上报自动注册为 ACTIVE）。 */
data class RegisterDeviceRequest(
    val deviceId: String,
    val userId: Long,
    val platform: String,
)
