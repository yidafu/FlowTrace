package dev.yidafu.flowtrace.process.segment

import dev.yidafu.flowtrace.process.config.ProcessorProperties
import dev.yidafu.flowtrace.shared.persistence.repository.DeviceStateLoader
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * 每设备状态的进程内 LRU（08 §8.1）。
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

    /** accessOrder=true：读也算访问，淘汰真正最久没用的设备。 */
    private val states = LinkedHashMap<String, DeviceState>(16, 0.75f, true)
    private val lock = ReentrantLock()

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
     * 取设备状态，未命中则回源加载（08 §8.1）。
     *
     * 并发要点：DB 的 IO **不在锁内**——并发消费线程（`flowtrace.processor.concurrency`）共用这一个 LRU，
     * 一次回源要跑三条 SQL，在锁里做会让所有设备的状态读取排在这条查询后面。代价是并发 miss 时
     * 可能重复回源，用锁内双重检查收敛成「后到者丢弃自己那份」，不额外加 per-key 的锁表。
     *
     * 淘汰同步落库（[flusher]）也在锁外，否则一次 flush 写库就顶住整个 store。
     *
     * @return 非 null；设备首次出现时返回空状态（无 lastKept、无 open）
     */
    fun getOrLoad(deviceId: String): DeviceState {
        // 快路径：命中直接返回
        lock.withLock { states[deviceId]?.let { return it } }

        // 缓存未命中（08 §12 指标 processor.state.cache.miss 的日志断点）：
        // 这里会走 3 条 SQL，日志能解释「为什么这一批特别慢」
        val loadStartNs = System.nanoTime()
        log.debug("设备状态缓存未命中，回源加载 deviceId={}", deviceId)

        // DB IO 放锁外，避免 32 个 listener 线程排队等一次查询
        val loaded = load(deviceId, props.openSegMaxPoints)
        log.debug(
            "设备状态加载完成 deviceId={} openPoints={} truncated={} lastKeptTsMs={} costMs={}",
            deviceId,
            loaded.open?.points?.size ?: 0,
            loaded.openTruncated,
            loaded.lastKept?.tsMs,
            (System.nanoTime() - loadStartNs) / NANOS_PER_MILLI,
        )

        val evicted: DeviceState? =
            lock.withLock {
                // 双重检查：并发 miss 时另一个线程可能已经放好了
                states[deviceId]?.let { return it }
                // 容量可能是 0/负数（配置写错）：firstOrNull 而不是 first，避免空表上抛 NoSuchElement
                val victim =
                    if (states.size >= props.deviceStateCacheSize) {
                        states.keys.firstOrNull()?.let { states.remove(it) }
                    } else {
                        null
                    }
                states[deviceId] = loaded
                victim
            }
        // 淘汰落库必须在锁外做：DB IO 不能占着全局锁
        evicted?.let { flusher(it) }
        return loaded
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

    /** 停机前排空：由应用的 shutdown hook 调用，避免重启丢掉所有 OPEN 尾段。 */
    fun flushAll() {
        val snapshot =
            lock.withLock {
                val copy = states.values.toList()
                states.clear()
                copy
            }
        snapshot.forEach { flusher(it) }
    }

    private companion object {
        const val NANOS_PER_MILLI = 1_000_000L
    }
}
