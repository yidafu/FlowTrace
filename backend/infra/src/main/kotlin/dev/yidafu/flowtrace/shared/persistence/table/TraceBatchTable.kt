package dev.yidafu.flowtrace.shared.persistence.table

import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

/**
 * trace_batch 表（03 §4，上报批次）。
 *
 * 按天 RANGE 分区（received_at），是分区主表的列映射事实源：
 * 主表 DDL 与按天子表由 PartitionManager（03 §7）创建，不纳入 SchemaUtils.create。
 * 幂等审计与对账用，非业务查询表；热路径幂等走 Redis SETNX（06 §3.4），本表只做落库审计。
 *
 * 主键含分区键 `(received_at, batch_id)`：PG 要求分区表的唯一约束必须覆盖全部分区键，
 * 因此**不能再声明仅含 `batch_id` 的唯一索引**（建表时会报
 * `unique constraint on partitioned table must include all partitioning columns`）。
 * batchId 的全局唯一由两处保证：Redis `idem:batch:{batchId}` SETNX 挡住重复上报，
 * 点级 `(ts, point_id)` 主键保证重复消费不产生重复数据；本表 `batch_id` 只建普通索引，
 * 供 `findByBatchId` / 状态回写定位（各子表各建一份）。
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
        // batchId 定位（幂等兜底在 Redis，见类注释；分区表上不能建仅含 batch_id 的唯一索引）
        index(customIndexName = "trace_batch_id_idx", false, batchId)
        // 失败批扫描（重处理入口）
        index(
            customIndexName = "trace_batch_failed_idx",
            false,
            status,
            filterCondition = { status inList listOf(3.toShort(), 4.toShort()) },
        )
    }
}
