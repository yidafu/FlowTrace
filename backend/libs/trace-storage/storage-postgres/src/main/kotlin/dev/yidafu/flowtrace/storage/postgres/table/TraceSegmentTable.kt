package dev.yidafu.flowtrace.storage.postgres.table

import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.javatime.timestamp

/**
 * trace_segment 表（03 §3，轨迹段含停留点，决策 D6）。
 *
 * 按月 RANGE 分区（start_ts），是分区主表的列映射事实源：
 * 主表 DDL 与按月子表由 PartitionManager（03 §7）创建，不纳入 SchemaUtils.create。
 * 抽稀派生数据，可由 trace_point 全量重算（D8/D13）。
 *
 * geometry 存 WKT 文本（MOVE=抽稀 LINESTRING / STAY=质心 POINT），空间计算在应用层 JTS，
 * 不在数据库做地理查询；bbox 四列供应用层粗筛免解析 WKT。
 */
object TraceSegmentTable : Table("trace_segment") {
    val startTs = timestamp("start_ts")
    val segmentId = uuid("segment_id")
    val userId = varchar("user_id", 64)
    val deviceId = varchar("device_id", 64)
    val sessionId = varchar("session_id", 64).nullable()
    val taskId = varchar("task_id", 64).nullable()
    val kind = short("kind")
    val moveMode = short("move_mode")
    val endTs = timestamp("end_ts")
    val pointCount = integer("point_count")
    val distanceM = double("distance_m")
    val geometry = text("geometry")
    val stayLatQ = integer("stay_lat_q").nullable()
    val stayLngQ = integer("stay_lng_q").nullable()
    val minLatQ = integer("min_lat_q")
    val maxLatQ = integer("max_lat_q")
    val minLngQ = integer("min_lng_q")
    val maxLngQ = integer("max_lng_q")
    val status = short("status")
    val updatedAt = timestamp("updated_at")

    override val primaryKey = PrimaryKey(startTs, segmentId)

    init {
        // 主查询：用户时间范围内的段
        index(customIndexName = "trace_segment_user_start_idx", false, userId, startTs)
        index(customIndexName = "trace_segment_session_idx", false, sessionId, filterCondition = { sessionId.isNotNull() })
        index(customIndexName = "trace_segment_task_idx", false, taskId, filterCondition = { taskId.isNotNull() })
        // processor 查找用户 OPEN 段
        index(
            customIndexName = "trace_segment_user_status_idx",
            false,
            userId,
            status,
            filterCondition = { status eq 1.toShort() },
        )
    }
}
