package dev.yidafu.flowtrace.storage.postgres.table

import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.CurrentTimestamp
import org.jetbrains.exposed.v1.javatime.timestamp

/**
 * trace_point 表（03 §2，原始轨迹点）。
 *
 * 按天 RANGE 分区（ts），是分区主表的列映射事实源：
 * 主表 CREATE TABLE ... PARTITION BY RANGE (ts) 与按天子表由 PartitionManager（03 §7）运行时 DDL 创建，
 * 本 Table 对象只供读写映射，不纳入 SchemaUtils.create（否则会建成非分区普通表）。
 *
 * 主键含分区键：point_id 内嵌时间戳（UUIDv5 "{batchId}:{idx}"），重复点 ts 相同 → 等价全局防重。
 * user_id 全链路字符串，varchar(64)，不做数值化。
 */
object TracePointTable : Table("trace_point") {
    val ts = timestamp("ts")
    val pointId = uuid("point_id")
    val userId = varchar("user_id", 64)
    val deviceId = varchar("device_id", 64)
    val batchId = uuid("batch_id")
    val sessionId = varchar("session_id", 64).nullable()
    val taskId = varchar("task_id", 64).nullable()
    val latQ = integer("lat_q")
    val lngQ = integer("lng_q")
    val accuracyCm = integer("accuracy_cm").nullable()
    val speedMps = float("speed_mps").nullable()
    val bearingDeg = short("bearing_deg").nullable()
    val altitudeCm = integer("altitude_cm").nullable()
    val locationSource = short("source").nullable()
    val flags = integer("flags").nullable().default(0)
    val receivedAt = timestamp("received_at").defaultExpression(CurrentTimestamp)

    override val primaryKey = PrimaryKey(ts, pointId)

    init {
        // 主查询路径：用户 + 时间范围
        index(customIndexName = "trace_point_user_ts_idx", false, userId, ts)
        // 设备对账/诊断
        index(customIndexName = "trace_point_device_ts_idx", false, deviceId, ts)
        // 会话查询（部分索引省空间）
        index(
            customIndexName = "trace_point_session_ts_idx",
            false,
            sessionId,
            ts,
            filterCondition = { sessionId.isNotNull() },
        )
    }
}
