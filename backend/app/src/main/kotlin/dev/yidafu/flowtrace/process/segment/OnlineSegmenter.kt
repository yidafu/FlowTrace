package dev.yidafu.flowtrace.process.segment

import dev.yidafu.flowtrace.process.config.ProcessorProperties
import dev.yidafu.flowtrace.process.pipeline.BatchMeta
import dev.yidafu.flowtrace.shared.model.GeoMath
import dev.yidafu.flowtrace.shared.model.MoveMode
import dev.yidafu.flowtrace.shared.model.SegmentStatus
import dev.yidafu.flowtrace.shared.model.TracePoint
import dev.yidafu.flowtrace.shared.persistence.repository.SegmentScope
import dev.yidafu.flowtrace.shared.persistence.repository.TraceSegmentRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * 在线分段：逐点判断窗口是否切分（时间间隔 / 跳变 / 运动模式变化），
 * 未闭合窗口在批末 flush（落 OPEN，内容与状态都没变时跳过），切分时闭合并落 FINALIZED。
 *
 * 设备状态由调用方传入（每批只 `getOrLoad` 一次）；本类对 [DeviceStateStore] 的另一处依赖
 * 是装配淘汰/停机回调（见 init）——窗口在内存里，进程一丢就再没人写它。
 *
 * 返回值是本次写入/重写的**段数**，供 [BatchPipeline] 汇总成一行批日志（08 §12 `segments`）。
 * 日志只打 ID/计数/时间戳与切分原因，**不打坐标与距离**（07 §5）。
 */
@Component
class OnlineSegmenter(
    private val deviceState: DeviceStateStore,
    private val props: ProcessorProperties,
    private val writer: TraceSegmentRepository,
    private val builder: SegmentBuilder,
    private val classifier: MoveModeClassifier,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    init {
        // 装配淘汰/停机回调（08 §8.1）：设备状态被 LRU 淘汰或进程停机时，把当前 OPEN 窗口按 OPEN 状态
        // 落库，下次 `getOrLoad` 重建窗口时续写（窗口被截断则由 `openTruncated` 先封口旧段）。
        // 不装配时 `DeviceStateStore` 只会打 error——内存窗口一丢，这段轨迹在库里就永远缺一段。
        // 注意 close=false：淘汰不是闭合语义，落 OPEN 才能续写，不能写成 FINALIZED。
        deviceState.flusher = { state -> flush(state, close = false) }
    }

    /**
     * 消费一批已清洗点，逐点决定「续写当前窗口 / 切开 / 开新窗口」，批末再视情况 flush 一次。
     *
     * 逐点判定三条切分条件（任一成立即切，可叠加，日志里以 `gap+jump+mode` 形式记录）：
     * 时间间隔 > `splitGapMs`、位移 > `splitJumpM`、运动模式变化（`MOVE_MODE` 由速度推断）。
     *
     * 两个「不动窗口」的边界：
     * - `ts <= 窗口末点 ts` 的乱序点**直接跳过**（`continue`）：段内点必须严格递增，
     *   插进去会让 WKT 折返、里程算重。这类点已落 `trace_point`，交给 `dirty_day` 重算补段；
     * - 窗口点数达 `openSegMaxPoints` 时**强制闭合**：单段无限增长会撑爆内存与单行 WKT（08 §8.5）。
     *
     * 批末 `flush(close = false)` 是把当前窗口按 OPEN 落库——不是闭合，下一批继续 replaceTail 延长它。
     * 但窗口是「整体重写」（stay 检测 + DP 抽稀 + WKT 拼串都是 O(窗口点数)），所以只在
     * **内容变了或状态该变**时才写：
     * - 本批一个点都没进窗口（重放批全被判重复/零岛、整批乱序）→ 尾段与库里完全一致，跳过；
     * - 尾段已按 FINALIZED 写过且没有新点 → 再写一次只会把同一行覆盖成同一结果，跳过。
     * 这两种情形在补传重放、弱网重复上报时并不罕见，跳过能省掉一次 replaceTail 与一次整窗重算。
     *
     * @param points 本批清洗后保留的点，按批内原始顺序（upload 展开时即 ts 升序；批内逆序点不清洗也不重排）
     * @param watermark 设备当前水位（调用方已在写库前更新过），仅用于判断窗口是否已「足够旧」可闭合
     * @param state 本批的设备状态，由调用方（[dev.yidafu.flowtrace.process.pipeline.BatchPipeline]）统一取一次并复用
     * @return 本次写入/重写的段数（含切分点前后的多次 flush）
     */
    fun onBatch(
        meta: BatchMeta,
        points: List<TracePoint>,
        watermark: Long?,
        state: DeviceState,
    ): Int {
        // 段身份来自 (userId, sessionId)：两者任一变化都必须作废上一轮草稿。
        // 否则复用的 segmentId 仍按旧身份派生，与本次写入的 user_id/session_id 列对不上，
        // 也和离线重算（按点里的身份重新派生）分叉，同一个窗口会出现两条互相打架的 id。
        if (state.userId != meta.userId || state.sessionId != meta.sessionId) {
            state.open?.drafts = emptyMap()
        }
        state.userId = meta.userId
        state.sessionId = meta.sessionId
        state.taskId = meta.taskId
        var written = 0
        // 本批是否往窗口里追加过点：没有则批末没必要重写整窗
        var appended = false
        for (point in points) {
            val open = state.open
            if (open != null) {
                val last = open.last
                if (point.tsMs <= last.tsMs) {
                    log.debug(
                        "processor 乱序点跳过分段 deviceId={} pointTsMs={} windowLastTsMs={}",
                        meta.deviceId,
                        point.tsMs,
                        last.tsMs,
                    )
                    continue
                }

                val gapMs = point.tsMs - last.tsMs
                val jumpM = GeoMath.haversineM(last.latQ, last.lngQ, point.latQ, point.lngQ)
                val mode = classifier.confirmedMode(point, open.moveMode)

                val split =
                    gapMs > props.splitGapMs ||
                        jumpM > props.splitJumpM ||
                        mode != open.moveMode

                if (split) {
                    val reason =
                        buildList {
                            if (gapMs > props.splitGapMs) add("gap")
                            if (jumpM > props.splitJumpM) add("jump")
                            if (mode != open.moveMode) add("mode")
                        }.joinToString("+")
                    log.debug(
                        "processor 分段切分 deviceId={} reason={} gapMs={} mode={}=>{}",
                        meta.deviceId,
                        reason,
                        gapMs,
                        open.moveMode,
                        mode,
                    )
                    // 切分点前后两条段都写库：flush(close=true) 会先把当前窗口闭合为 FINALIZED
                    written += flush(state, close = true, watermark = watermark)
                }
            }

            // 新窗口必须带着首点构造：[OpenWindow] 约定窗口至少一个点，先建空窗口再 append 会直接抛
            val window = state.open
            if (window == null) {
                state.open =
                    OpenWindow(
                        startTsMs = point.tsMs,
                        points = mutableListOf(point),
                        moveMode = classifier.confirmedMode(point, MoveMode.UNKNOWN),
                    )
            } else {
                window.points += point
            }
            appended = true
            state.tailFinalized = false
            val active = state.open
            if (active != null && active.points.size >= props.openSegMaxPoints) {
                log.debug(
                    "processor 分段窗口达到上限，强制闭合 deviceId={} maxPoints={}",
                    meta.deviceId,
                    props.openSegMaxPoints,
                )
                written += flush(state, close = true, watermark = watermark)
            }
        }

        if (appended || state.openTruncated || tailNeedsSeal(state, watermark)) {
            written += flush(state, close = false, watermark = watermark)
        }
        return written
    }

    /**
     * 尾段是否还需要一次「封口」写入：内容是旧的（没有新点），但末点已落在水位之外，
     * 状态该从 OPEN 翻成 FINALIZED。已经封过（[DeviceState.tailFinalized]）就不再重复写。
     */
    private fun tailNeedsSeal(
        state: DeviceState,
        watermark: Long?,
    ): Boolean {
        if (state.tailFinalized) return false
        val open = state.open ?: return false
        return watermark != null && open.last.tsMs < watermark - props.watermarkDriftMs
    }

    /**
     * 把当前 OPEN 窗口落库（`replaceTail`）：先清掉该窗口派生过的旧段，再写入本次构建的段草稿，
     * 因此同一个窗口被反复 flush 不会产生重复段。
     *
     * 两处增量（窗口点上限 5000，整窗 O(n) 是长驻设备的真实负担）：
     * - **算**：[OpenWindow.drafts] 作为 `reusable` 传给 [SegmentBuilder.build]，区间没变的段直接复用，
     *   只重算被新点拉长的尾巴区间；
     * - **写**：只有在**状态要翻**（`close = true` 或水位已落后 → FINALIZED）时才从窗口起点整窗重写——
     *   同一次 `replaceTail` 的 status 作用于它写入的每一行，漏写的前缀会永远停在 OPEN；
     *   普通的 OPEN 续写从**第一个重算过的段**开始写，前缀行原样留在库里。
     *
     * @param close true = 闭合（段落 FINALIZED 并清空窗口，切分/点数到顶时用）；
     *   false = 续写（段落 OPEN 保留窗口，批末与 LRU 淘汰时用）。**淘汰不是闭合语义**，
     *   传 true 会让这段轨迹在库里永久封口、下一批只能另起一段
     * @param watermark 非空且窗口末点已落后水位超过乱序窗口时，即使 `close = false` 也落 FINALIZED——
     *   长时间没有新批的设备，其窗口靠这个条件最终封口，不必等下一批
     * @return 本次实际写入的段数；无窗口、无 userId（状态未带上身份）或整窗复用时为 0
     */
    fun flush(
        state: DeviceState,
        close: Boolean = false,
        watermark: Long? = null,
    ): Int {
        val open = state.open ?: return 0
        val userId = state.userId ?: return 0
        val scope =
            SegmentScope(
                userId = userId,
                sessionId = state.sessionId,
                deviceId = state.deviceId,
            )
        if (state.openTruncated) {
            log.debug(
                "processor 截断窗口恢复：先封口旧 OPEN 段 deviceId={} beforeTsMs={}",
                state.deviceId,
                open.startTsMs,
            )
            writer.finalizeOpenBefore(scope, open.startTsMs)
            state.openTruncated = false
        }

        val drafts =
            builder.build(
                userId = userId,
                sessionId = state.sessionId,
                points = open.points,
                epsilonM = props.dpStorageEpsilonM,
                stayRadiusM = props.stayRadiusM,
                stayMinMs = props.stayMinMs,
                stayMergeGapMs = props.stayMergeGapMs,
                reusable = open.drafts,
            )

        val finalized = close || (watermark != null && open.last.tsMs < watermark - props.watermarkDriftMs)

        // 写库范围：**只有状态要翻**（闭合 / 水位已落后）时才必须从窗口起点整窗重写，
        // 因为同一次 replaceTail 的 status 作用于它写入的每一行，漏写的前缀会永远停在 OPEN。
        // 普通的 OPEN 续写只需从**第一个重算过的段**开始：前面的段内容没变，库里已经是它们，
        // 删了再写一遍纯属浪费（一次 flush 可能涉及几十个段、每个都带 WKT）。
        val fromTsMs =
            if (finalized) {
                open.startTsMs
            } else {
                drafts.firstOrNull { open.drafts[it.startTsMs] !== it }?.startTsMs ?: return 0
            }
        val toWrite = if (fromTsMs == open.startTsMs) drafts else drafts.filter { it.startTsMs >= fromTsMs }

        writer.replaceTail(scope, fromTsMs, drafts = toWrite, status = if (finalized) SegmentStatus.FINALIZED else SegmentStatus.OPEN)
        // 本轮草稿表留作下一轮的复用基线
        open.drafts = drafts.associateBy { it.startTsMs }
        // 记下尾段当前状态：内容没变时，批末不必为了「封口」再重写一次（见 onBatch 的 flush 门控）
        state.tailFinalized = finalized
        log.debug(
            "processor 分段写入 deviceId={} userId={} windowStartTsMs={} windowPoints={} fromTsMs={} segments={}/{} status={} close={}",
            state.deviceId,
            userId,
            open.startTsMs,
            open.points.size,
            fromTsMs,
            toWrite.size,
            drafts.size,
            if (finalized) SegmentStatus.FINALIZED else SegmentStatus.OPEN,
            close,
        )
        if (close) {
            state.open = null
            state.tailFinalized = false
        }
        return toWrite.size
    }
}
