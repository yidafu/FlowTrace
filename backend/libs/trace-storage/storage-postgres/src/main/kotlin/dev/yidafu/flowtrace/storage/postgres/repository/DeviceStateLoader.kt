package dev.yidafu.flowtrace.storage.postgres.repository

import dev.yidafu.flowtrace.model.SegmentStatus
import dev.yidafu.flowtrace.model.TracePoint
import dev.yidafu.flowtrace.storage.postgres.table.TracePointTable
import dev.yidafu.flowtrace.storage.postgres.table.TraceSegmentTable
import dev.yidafu.flowtrace.storage.postgres.table.toDomain
import org.jetbrains.exposed.v1.core.SortOrder
import org.jetbrains.exposed.v1.core.and
import org.jetbrains.exposed.v1.jdbc.select
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.springframework.stereotype.Repository
import java.time.Instant

@Repository
class DeviceStateLoader {
    fun loadLastKept(deviceId: String): TracePoint? =
        transaction {
            TracePointTable
                .selectAll()
                .where { TracePointTable.deviceId eq deviceId }
                .orderBy(TracePointTable.ts to SortOrder.DESC)
                .limit(1)
                .map { it.toDomain() }
                .singleOrNull()
        }

    fun loadOpenStartTs(
        userId: String,
        deviceId: String,
    ): Instant? =
        transaction {
            TraceSegmentTable
                .select(TraceSegmentTable.startTs)
                .where {
                    (TraceSegmentTable.deviceId eq deviceId) and
                        (TraceSegmentTable.userId eq userId) and
                        (TraceSegmentTable.status eq SegmentStatus.OPEN.value)
                }.orderBy(TraceSegmentTable.startTs to SortOrder.DESC)
                .limit(1)
                .map { it[TraceSegmentTable.startTs] }
                .singleOrNull()
        }

    fun loadWindow(
        userId: String,
        startTs: Instant,
        maxPoints: Int,
    ): List<TracePoint> =
        transaction {
            TracePointTable
                .selectAll()
                .where {
                    (TracePointTable.userId eq userId) and
                        (TracePointTable.ts greaterEq startTs)
                }.orderBy(TracePointTable.ts to SortOrder.DESC)
                .limit(maxPoints)
                .map { it.toDomain() }
                .reversed()
        }
}
