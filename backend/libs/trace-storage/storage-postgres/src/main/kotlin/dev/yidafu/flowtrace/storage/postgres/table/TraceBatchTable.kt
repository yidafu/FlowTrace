package dev.yidafu.flowtrace.storage.postgres.table

import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

/**
 * trace_batch 表（03 §4，上报批次）。
 *
 * 按天 RANGE 分区（received_at），是分区主表的列映射事实源：
 * 主表 DDL 与按天子表由 PartitionManager（03 §7）创建，不纳入 SchemaUtils.create。
 * 幂等审计与对账用，非业务查询表；热路径幂等走 Redis SETNX，本表唯一索引是兜底与审计。
 *
 * 主键含分区键；received_at 唯一即 batch_id 唯一（一个 batchId 只有一个 received_at），
 * 用唯一索引显式声明 batchId 幂等语义。
 */
object TraceBatchTable : Table("trace_batch") {
    val receivedAt = timestamp("received_at")
    val batchId = uuid("batch_id")
    val userId = varchar("user_id", 64)
    val deviceId = varchar("device_id", 64)
    val channel = short("channel")
    val pointCount = integer("point_count")
    val firstTs = timestamp("first_ts")
    val lastTs = timestamp("last_ts")
    val clientTs = timestamp("client_ts")
    val sdkVersion = varchar("sdk_version", 32)
    val platform = varchar("platform", 16)
    val status = short("status")
    val errorCode = varchar("error_code", 64).nullable()
    val processedAt = timestamp("processed_at").nullable()

    override val primaryKey = PrimaryKey(receivedAt, batchId)

    init {
        index(customIndexName = "trace_batch_device_received_idx", false, deviceId, receivedAt)
        // batchId 全局唯一（幂等兜底）
        index(customIndexName = "trace_batch_id_uidx", true, batchId)
        // 失败批扫描（重处理入口）
        index(
            customIndexName = "trace_batch_failed_idx",
            false,
            status,
            filterCondition = { status inList listOf(3.toShort(), 4.toShort()) },
        )
    }
}
