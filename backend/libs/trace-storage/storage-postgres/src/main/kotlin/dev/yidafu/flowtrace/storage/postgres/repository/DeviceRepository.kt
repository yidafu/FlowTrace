package dev.yidafu.flowtrace.storage.postgres.repository

import dev.yidafu.flowtrace.model.DeviceStatus
import dev.yidafu.flowtrace.storage.postgres.table.DeviceTable
import org.jetbrains.exposed.v1.core.SqlExpressionBuilder.eq
import org.jetbrains.exposed.v1.jdbc.insertIgnore
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.springframework.stereotype.Repository
import java.time.Instant

/** device 表只读查询（upload 设备状态检查用）。 */
@Repository
open class DeviceRepository {
    /**
     * 查设备状态；设备不存在返回 null。
     *
     * upload 读取路径调本方法；null 时由调用方决定是否 [registerIfAbsent]（04 §5 首次合法上报自动注册）。
     */
    fun findStatus(deviceId: String): DeviceStatus? = transaction { findStatusInTx(deviceId) }

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

    /** 当前事务内查询，避免 registerIfAbsent 内嵌 transaction。 */
    private fun findStatusInTx(deviceId: String): DeviceStatus? =
        DeviceTable
            .selectAll()
            .where { DeviceTable.deviceId eq deviceId }
            .map { (DeviceStatus.from(it[DeviceTable.status])) }
            .singleOrNull()
}
