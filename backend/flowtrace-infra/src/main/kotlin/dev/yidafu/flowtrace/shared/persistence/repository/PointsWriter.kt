package dev.yidafu.flowtrace.shared.persistence.repository

import dev.yidafu.flowtrace.shared.model.TracePoint
import org.springframework.stereotype.Component

/**
 * processor 热路径上的幂等写点入口。
 *
 * 直接委托 [TracePointRepository.batchInsertIgnore]：两者原先是各写一份列映射的重复实现，
 * 2026-11-21 写点改为单条多行 INSERT（`ON CONFLICT DO NOTHING`）后收敛到 Repository 一处——
 * 逐行 JDBC batch 在 `ON CONFLICT` 下不被 pgjdbc 的 `reWriteBatchedInserts` 改写，
 * 实测 10 倍耗时（docs/design/09-performance.md §9 与 2026-11-21 复测报告 §8）。
 *
 * 返回「尝试写入」行数（含被冲突丢弃的），不另加 SELECT 计数，真实重复率靠离线对账。
 */
@Component
class PointsWriter(
    private val repository: TracePointRepository,
) {
    fun batchInsertIgnore(rows: List<TracePoint>): Int = repository.batchInsertIgnore(rows)
}
