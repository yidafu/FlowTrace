package dev.yidafu.flowtrace.admin

import dev.yidafu.flowtrace.admin.dto.RegisterDeviceRequest
import dev.yidafu.flowtrace.admin.dto.UpdateDeviceStatusRequest
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

/**
 * 设备管理控制器（docs/design/04-api.md §5，scope=trace:admin）。
 * 空实现占位：只定义路由契约，管理逻辑后续补充。
 *
 * 设备状态（03 §5）：ACTIVE / DEGRADED（限流自动降级）/ SUSPENDED / DISABLED；
 * SUSPENDED/DISABLED 后 upload 拒收（设备状态缓存 TTL 60s，06 §3.2）。
 */
@RestController
@RequestMapping("/v1/admin/devices")
class DeviceAdminController {
    /** 设备列表：GET /v1/admin/devices?userId=&status=&cursor= */
    @GetMapping
    fun listDevices(
        @RequestParam("userId", required = false) userId: Long?,
        @RequestParam("status", required = false) status: String?,
        @RequestParam("cursor", required = false) cursor: String?,
    ): ResponseEntity<Void> = TODO("待实现：设备列表查询（游标分页）")

    /**
     * 预注册设备：POST /v1/admin/devices（可选流程；
     * 未注册设备首次合法上报自动注册为 ACTIVE）。
     */
    @PostMapping
    fun registerDevice(
        @RequestBody request: RegisterDeviceRequest,
    ): ResponseEntity<Void> = TODO("待实现：设备预注册")

    /** 设备状态变更：PATCH /v1/admin/devices/{deviceId}（SUSPENDED/DISABLED 拒收，恢复 ACTIVE）。 */
    @PatchMapping("/{deviceId}")
    fun updateDeviceStatus(
        @PathVariable("deviceId") deviceId: String,
        @RequestBody request: UpdateDeviceStatusRequest,
    ): ResponseEntity<Void> = TODO("待实现：设备状态变更（刷新状态缓存）")
}
