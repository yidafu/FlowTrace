package dev.yidafu.flowtrace.process.segment

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.RemovalListener
import dev.yidafu.flowtrace.process.config.ProcessorProperties
import dev.yidafu.flowtrace.shared.persistence.repository.DeviceStateLoader
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * 每设备状态的进程内 LRU（08 §8.1）：Caffeine `maximumSize`，不再手写 `LinkedHashMap` + 全局锁。
 *
 * 关键语义是**淘汰落库**：设备状态持有未闭合窗口，被 SIZE 淘汰时必须先按 OPEN 状态写回
 * （[flusher]，由 [OnlineSegmenter] 装配），否则内存窗口一丢，库里就永远缺一段。所以用
 * `evictionListener` 而不是 `removalListener`——前者对 SIZE 淘汰**在淘汰线程上同步执行**
 * （Caffeine 3.2.x），`getOrLoad` 返回时被淘汰设备的窗口已经落库；后者走 executor 异步分发，
 * 停机时可能丢通知。回调异常由 Caffeine 捕获记日志后吞掉，正合适：淘汰落库是幂等的安全网
 * （批末 flush 已写过一遍，这里草稿全量复用基本是零写入的空转），失败打日志即可，
 * 不值得让整批失败。
 *
 * 并发模型（对比旧实现的 `ReentrantLock`）：
 * - 读命中无锁直接返回——旧实现每次访问都要抢全局锁；
 * - miss 回源在 key 所属 hash bin 的锁内执行：同设备并发 miss 只回源一次（旧实现的双重检查
 *   让后到者白付三条 SQL 再丢弃）；不同设备互不影响，hash 碰撞才需要排队，回源不再占住全局锁；
 * - executor 用直连（`Runnable::run`）：让维护任务（含淘汰）在触发写入的线程内联完成——
 *   默认 executor（ForkJoinPool）会把淘汰异步化，淘汰落库就可能在 `getOrLoad` 返回之后才发生，
 *   与「淘汰必须先 flush」的语义冲突。容量未满的稳态下不发生淘汰，直连 executor 没有额外开销。
 *
 * 为什么 `load()` 在本类而不在 [DeviceStateLoader]：loader 位于 libs/storage-postgres，
 * 不能反向依赖 services/processor 的 [DeviceState]。loader 只提供三条独立 SQL，
 * 「把三条结果组装成状态」是 processor 的业务逻辑，自然落在这里。
 */
@Component
class DeviceStateStore(
    private val props: ProcessorProperties,
    private val loader: DeviceStateLoader,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * 淘汰/停机时把 OPEN 段落库的回调，由 [OnlineSegmenter] 在 `init` 中装配（`close = false`）。
     *
     * 不放在构造参数里：函数类型不是 Spring bean，注入会失败；用可变属性让 segmenter 反向注册、
     * 也避免 store ↔ segmenter 的构造依赖成环。未装配时淘汰会丢 OPEN 窗口——打 error 日志，
     * 属配置错误（正常启动流程里 segmenter 一定会覆盖它）。
     */
    var flusher: (DeviceState) -> Unit = { state ->
        log.error("DeviceState 淘汰但未装配 flusher，OPEN 段落库缺失: deviceId={}", state.deviceId)
    }

    /**
     * 容量可能是 0/负数（配置写错）：clamp 到 0（Caffeine 对负数直接抛 IAE），
     * 0 = 不缓存，每次访问都回源；1 起才是常规 LRU 行为。
     */
    private val cache: Cache<String, DeviceState> =
        Caffeine
            .newBuilder()
            .maximumSize(props.deviceStateCacheSize.coerceAtLeast(0).toLong())
            .executor { it.run() }
            // 显式 SAM 类型：evictionListener 的 <K1, V1> 从未标注 lambda 推断不出来；
            // value 理论上可空（Caffeine 标了 @Nullable），为 null 即无状态可 flush，直接跳过
            .evictionListener(RemovalListener<String, DeviceState> { _, state, _ -> state?.let(flusher) })
            .build()

    /**
     * 取设备状态，未命中则回源加载（08 §8.1）。
     *
     * @return 非 null；设备首次出现时返回空状态（无 lastKept、无 open）
     */
    fun getOrLoad(deviceId: String): DeviceState =
        cache.get(deviceId) { devId ->
            // 缓存未命中（08 §12 指标 processor.state.cache.miss 的日志断点）：
            // 这里会走 3 条 SQL，日志能解释「为什么这一批特别慢」
            val loadStartNs = System.nanoTime()
            log.debug("设备状态缓存未命中，回源加载 deviceId={}", devId)

            val loaded = load(devId, props.openSegMaxPoints)
            log.debug(
                "设备状态加载完成 deviceId={} openPoints={} truncated={} lastKeptTsMs={} costMs={}",
                devId,
                loaded.open?.points?.size ?: 0,
                loaded.openTruncated,
                loaded.lastKept?.tsMs,
                (System.nanoTime() - loadStartNs) / NANOS_PER_MILLI,
            )
            loaded
        }

    /**
     * 缓存 miss 的状态恢复（08 §8.1）：
     * ① 最近点 → lastKept + userId；② OPEN 段起点；③ 窗口点列重建。
     *
     * [maxPoints] 截断时把 [DeviceState.openTruncated] 置位，由 segmenter 在下次 flush
     * 时先把旧 OPEN 段 FINALIZE，再从内存首点重写——否则 replaceTail 的删除范围会漏掉旧段。
     */
    private fun load(
        deviceId: String,
        maxPoints: Int,
    ): DeviceState {
        val last = loader.loadLastKept(deviceId) ?: return DeviceState(deviceId)
        val state =
            DeviceState(
                deviceId = deviceId,
                userId = last.userId,
                sessionId = last.sessionId,
                taskId = last.taskId,
                lastKept = last,
            )

        val openStart = loader.loadOpenStartTs(last.userId, deviceId) ?: return state
        val points = loader.loadWindow(last.userId, openStart, maxPoints)
        if (points.isEmpty()) return state

        state.openTruncated =
            points.size >= maxPoints &&
            points.first().tsMs > openStart.toEpochMilli()
        state.open = OpenWindow(points.first().tsMs, points.toMutableList())
        return state
    }

    /**
     * 停机前排空：由应用的 shutdown hook 调用，避免重启丢掉所有 OPEN 尾段。
     *
     * 显式 flush 而不是依赖淘汰回调：停机清空是 EXPLICIT 移除，不走 SIZE 淘汰路径，
     * `evictionListener` 不感知。快照后逐个 flush——此时 consumer 线程已停，无并发写入。
     */
    fun flushAll() {
        val snapshot = cache.asMap().values.toList()
        cache.invalidateAll()
        snapshot.forEach { flusher(it) }
    }

    private companion object {
        const val NANOS_PER_MILLI = 1_000_000L
    }
}
