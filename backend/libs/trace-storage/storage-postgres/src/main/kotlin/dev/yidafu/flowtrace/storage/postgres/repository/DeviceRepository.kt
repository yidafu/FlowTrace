package dev.yidafu.flowtrace.storage.postgres.repository

import dev.yidafu.flowtrace.model.DeviceStatus
import dev.yidafu.flowtrace.storage.postgres.table.DeviceTable
import org.jetbrains.exposed.v1.core.ResultRow
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.eq
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.greater
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.jdbc.insertIgnore
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import org.springframework.stereotype.Repository
import java.time.Instant

/** `device` 的一行（03 §5，admin 域）。 */
data class DeviceRow(
    val deviceId: String,
    val userId: String,
    val platform: String?,
    val appVersion: String?,
    val sdkVersion: String?,
    val status: DeviceStatus,
    val lastSeenAt: Instant?,
    val createdAt: Instant,
)

/** device 表读写仓储（upload 设备状态检查 + admin 设备管理）。 */
@Repository
open class DeviceRepository {
    /**
     * 查设备状态；设备不存在返回 null。
     *
     * upload 读取路径调本方法；null 时由调用方决定是否 [registerIfAbsent]（04 §5 首次合法上报自动注册）。
     */
    fun findStatus(deviceId: String): DeviceStatus? = transaction { findStatusInTx(deviceId) }

    /** 按主键查设备全量字段（admin 详情/排查用）。 */
    fun findById(deviceId: String): DeviceRow? =
        transaction {
            DeviceTable
                .selectAll()
                .where { DeviceTable.deviceId eq deviceId }
                .map { it.toDeviceRow() }
                .singleOrNull()
        }

    /**
     * 设备列表（`GET /v1/admin/devices`），按 device_id 升序 keyset 分页。
     *
     * @param userId 可选归属过滤（走 `device_user_id_idx`）
     * @param status 可选状态过滤
     * @param afterDeviceId 上一页最后一个 device_id，null 表示首页
     */
    fun list(
        userId: String? = null,
        status: DeviceStatus? = null,
        limit: Int = DEFAULT_LIMIT,
        afterDeviceId: String? = null,
    ): List<DeviceRow> =
        transaction {
            val filters =
                listOfNotNull(
                    userId?.let { DeviceTable.userId eq it },
                    status?.let { DeviceTable.status eq it.value },
                    afterDeviceId?.let { DeviceTable.deviceId greater it },
                )
            val base = DeviceTable.selectAll()
            val filtered = if (filters.isEmpty()) base else base.where(filters.reduce { acc, op -> acc and op })
            filtered
                .orderBy(DeviceTable.deviceId to SortOrder.ASC)
                .limit(limit)
                .map { it.toDeviceRow() }
        }

    /**
     * 注册设备（幂等），返回注册后/已存在设备的真实状态。
     *
     * 语义：INSERT ... ON CONFLICT (device_id) DO NOTHING——并发首次上报只有一个能插入，
     * 其余命中冲突被忽略。插入后回读状态而非直接返回 ACTIVE：并发下若另一请求已插入
     * 并置为 SUSPENDED/DISABLED，直接当 ACTIVE 会绕过禁用检查。
     *
     * 字段来源必须是网关注入的可信身份头，不能取请求体（防串号）。status 固定 ACTIVE（04 §5），
     * created_at / last_seen_at 取当前时刻（两列均无 DB 默认值）。
     *
     * @return 设备状态；正常为 ACTIVE，已存在且被禁用则为对应禁用状态（调用方需据此拒收）
     */
    fun registerIfAbsent(
        deviceId: String,
        userId: String,
        platform: String? = null,
        appVersion: String? = null,
        sdkVersion: String? = null,
    ): DeviceStatus =
        transaction {
            val now = Instant.now()
            DeviceTable.insertIgnore {
                it[DeviceTable.deviceId] = deviceId
                it[DeviceTable.userId] = userId
                it[DeviceTable.platform] = platform
                it[DeviceTable.appVersion] = appVersion
                it[DeviceTable.sdkVersion] = sdkVersion
                it[DeviceTable.status] = DeviceStatus.ACTIVE.value
                it[DeviceTable.lastSeenAt] = now
                it[DeviceTable.createdAt] = now
            }
            findStatusInTx(deviceId) ?: DeviceStatus.ACTIVE
        }

    /**
     * 刷新设备最近上报信息（`last_seen_at` 与版本字段）。
     *
     * 只在传入非 null 时覆盖版本列：SDK 若没带版本头，不能把已有值刷成 NULL。
     *
     * @return 影响行数（0 表示设备不存在）
     */
    fun touchSeen(
        deviceId: String,
        at: Instant = Instant.now(),
        platform: String? = null,
        appVersion: String? = null,
        sdkVersion: String? = null,
    ): Int =
        transaction {
            DeviceTable.update({ DeviceTable.deviceId eq deviceId }) {
                it[DeviceTable.lastSeenAt] = at
                platform?.let { value -> it[DeviceTable.platform] = value }
                appVersion?.let { value -> it[DeviceTable.appVersion] = value }
                sdkVersion?.let { value -> it[DeviceTable.sdkVersion] = value }
            }
        }

    /**
     * 变更设备状态（`PATCH /v1/admin/devices/{deviceId}`）。
     *
     * SUSPENDED/DISABLED 后 upload 拒收（06 §3.2）；调用方还需写 `cfg:devver:{deviceId}`
     * 主动失效 upload 的本地状态缓存（06 §6）。
     *
     * @return 影响行数（0 表示设备不存在）
     */
    fun updateStatus(
        deviceId: String,
        status: DeviceStatus,
    ): Int =
        transaction {
            DeviceTable.update({ DeviceTable.deviceId eq deviceId }) {
                it[DeviceTable.status] = status.value
            }
        }

    /** 当前事务内查询，避免 registerIfAbsent 内嵌 transaction。 */
    private fun findStatusInTx(deviceId: String): DeviceStatus? =
        DeviceTable
            .selectAll()
            .where { DeviceTable.deviceId eq deviceId }
            .map { (DeviceStatus.from(it[DeviceTable.status])) }
            .singleOrNull()

    private fun ResultRow.toDeviceRow(): DeviceRow =
        DeviceRow(
            deviceId = this[DeviceTable.deviceId],
            userId = this[DeviceTable.userId],
            platform = this[DeviceTable.platform],
            appVersion = this[DeviceTable.appVersion],
            sdkVersion = this[DeviceTable.sdkVersion],
            status = DeviceStatus.from(this[DeviceTable.status]),
            lastSeenAt = this[DeviceTable.lastSeenAt],
            createdAt = this[DeviceTable.createdAt],
        )

    companion object {
        /** 设备列表默认页大小。 */
        const val DEFAULT_LIMIT: Int = 100
    }
}
