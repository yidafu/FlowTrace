package dev.yidafu.flowtrace.process.segment

import dev.yidafu.flowtrace.process.config.ProcessorProperties
import dev.yidafu.flowtrace.shared.persistence.repository.TraceSegmentRepository
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.TimeUnit

/**
 * OPEN 段的兜底收尾（06 §3.4「另有定时扫描兜底」）。
 *
 * 在线链路的正常收尾条件是「窗口末点落后水位超过乱序窗口」——它依赖**下一批数据到达**来触发。
 * 设备彻底沉默（下班关机、卸载 App、进飞行模式）时不会再有下一批，那段轨迹就永久停在 OPEN：
 * 查询侧把它当「未完成段」，里程接口还得降级到点级 haversine，代价明显。
 *
 * 判定口径与在线收尾保持一致（`end_ts < now − watermarkDriftMs`）：
 * - 用 `now` 而不是设备水位：本作业跨设备批量扫描，读每台设备的水位得不偿失；
 *   多等一个乱序窗口才收尾对查询没有实质影响（查询本来就不依赖段是否 FINALIZED 来判断新鲜度）；
 * - 只封口、不重算：段内容由在线链路写好的，这里只改状态，避免兜底作业变成第二个写路径。
 *
 * 幂等：重复扫描只影响 `status=OPEN` 的行，封过的段不会再命中（走 `trace_segment_user_status_idx`）。
 */
@Component
class SegmentFinalizeScanner(
    private val props: ProcessorProperties,
    private val segmentRepository: TraceSegmentRepository,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 每 [ProcessorProperties.segmentFinalizeIntervalSec] 扫一次，一轮内分批取到扫空为止。
     *
     * 用 `while` 连续取而不是「一轮一批」：积压时（比如长时间停机后重启）单批 500 条追不回来。
     * 单批上限的作用是框住单次事务的锁行数，不是限制总吞吐。
     */
    @Scheduled(
        fixedDelayString = $$"${flowtrace.processor.segment-finalize-interval-sec:300}",
        timeUnit = TimeUnit.SECONDS,
    )
    fun scan() {
        val threshold = System.currentTimeMillis() - props.watermarkDriftMs
        var total = 0
        while (true) {
            val closed =
                try {
                    segmentRepository.finalizeStaleOpen(threshold, props.segmentFinalizeBatch)
                } catch (ex: Exception) {
                    // DB 抖动：本轮放弃，下个周期再来（不能把异常抛给调度线程，否则后续调度一起停）
                    log.error("processor 段收尾扫描失败，本轮跳过 cause={}", ex.message)
                    break
                }
            total += closed
            if (closed < props.segmentFinalizeBatch) break
        }
        if (total > 0) {
            log.info("processor 段收尾完成 closed={} thresholdTsMs={}", total, threshold)
        }
    }
}
