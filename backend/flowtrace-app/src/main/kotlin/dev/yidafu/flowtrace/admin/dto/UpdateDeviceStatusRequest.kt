package dev.yidafu.flowtrace.admin.dto

/** 设备状态变更请求体（PATCH /v1/admin/devices/{deviceId}；SUSPENDED/DISABLED 拒收，恢复 ACTIVE）。 */
data class UpdateDeviceStatusRequest(
    val status: String,
)
