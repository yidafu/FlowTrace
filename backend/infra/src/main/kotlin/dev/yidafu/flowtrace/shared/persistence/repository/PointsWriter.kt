package dev.yidafu.flowtrace.shared.persistence.repository

import dev.yidafu.flowtrace.shared.model.TracePoint
import dev.yidafu.flowtrace.shared.persistence.table.TracePointTable
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.springframework.stereotype.Component
import java.time.Instant

@Component
class PointsWriter {
    /**
     * processor 热路径上的幂等写点入口。
     *
     * 与 `TracePointRepository.batchInsertIgnore` 使用同一张表、同一套列映射；
     * 差异只在于本类面向 processor 已清洗好的 [TracePoint] 列表，不做查询。
     *
     * `ignore = true` 对应 `ON CONFLICT DO NOTHING`；返回的是「尝试写入」行数，
     * 不是实际插入行数（JDBC batch 下冲突行无法逐行计数）。
     */
    fun batchInsertIgnore(rows: List<TracePoint>): Int {
        if (rows.isEmpty()) return 0
        return transaction {
            rows.chunked(BATCH_SIZE).sumOf { chunk ->
                TracePointTable
                    .batchInsert(chunk, ignore = true, shouldReturnGeneratedValues = false) { p ->
                        this[TracePointTable.ts] = Instant.ofEpochMilli(p.tsMs)
                        this[TracePointTable.pointId] = p.pointId
                        this[TracePointTable.userId] = p.userId
                        this[TracePointTable.deviceId] = p.deviceId
                        this[TracePointTable.batchId] = p.batchId
                        this[TracePointTable.sessionId] = p.sessionId
                        this[TracePointTable.taskId] = p.taskId
                        this[TracePointTable.latQ] = p.latQ
                        this[TracePointTable.lngQ] = p.lngQ
                        this[TracePointTable.accuracyCm] = p.accuracyCm
                        this[TracePointTable.speedMps] = p.speedMps
                        this[TracePointTable.bearingDeg] = p.bearingDeg
                        this[TracePointTable.altitudeCm] = p.altitudeCm
                        this[TracePointTable.locationSource] = p.source?.value?.toShort()
                        this[TracePointTable.flags] = p.flags
                    }.size
            }
        }
    }

    companion object {
        /** JDBC batch 分块大小：与 03 §2「JDBC batch 1000」一致。 */
        const val BATCH_SIZE: Int = 1000
    }
}
