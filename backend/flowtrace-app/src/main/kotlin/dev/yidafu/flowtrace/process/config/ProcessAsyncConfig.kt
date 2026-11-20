package dev.yidafu.flowtrace.process.config

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory

/**
 * processor 侧异步件的协程调度器（08 §6.3）。
 *
 * 与 upload 的 `UploadAsyncConfig` 同一套思路：协程不是「免线程池」，阻塞点（JDBC）必须落在
 * **专用**线程上，否则 `Dispatchers.IO` 会把 query/admin 的 IO 一起挤掉。
 *
 * 这里只给一条**单线程**调度器：`trace_batch` 的状态更新按 batchId 会扫全部分区，
 * 并发度钉死在 1 才能避免这类重查询同时压到 DB（见 `BatchStatusUpdater` 的类注释）。
 *
 * 停机顺序：`BatchStatusUpdater` 依赖本 bean，Spring 先销毁它（排空队列）再 `close()` 调度器，
 * 所以这里的 `destroyMethod = "close"` 不会打断排空。
 */
@Configuration(proxyBeanMethods = false)
class ProcessAsyncConfig {
    /** 批状态更新专用调度器；bean 名必须与 [STATUS_DISPATCHER] 一致——改名即断链（启动期即报错）。 */
    @Bean(STATUS_DISPATCHER, destroyMethod = "close")
    fun batchStatusDispatcher(): CoroutineDispatcher = statusThreadPool().asCoroutineDispatcher()

    companion object {
        /** 状态更新调度器的 bean 名；`BatchStatusUpdater` 按名注入。 */
        const val STATUS_DISPATCHER = "batchStatusDispatcher"

        /** 有界队列容量：满即丢弃并告警（旁路台账不能反压消费线程）。 */
        const val QUEUE_CAPACITY = 2048

        /**
         * 单次回写的批大小：攒够（或队列暂时空了）就合成一条 `UPDATE ... WHERE batch_id IN (...)`。
         * 100 条 ≈ 2 个消费批的量，够把提交次数从「每批 1 次」压到「每百批 1 次」，
         * 又不至于让一条语句的 IN 列表过长（PG 绑定参数上限 65535，远未触及）。
         */
        const val STATUS_BATCH_SIZE = 100

        /** 停机排空上限：正常重启时把队列里已 ack 的状态流转写完。 */
        const val DRAIN_TIMEOUT_MS = 5_000L

        /** 单线程池：线程名固定，线程 dump 里一眼能认出是批状态更新。 */
        fun statusThreadPool(): ExecutorService {
            val factory = ThreadFactory { runnable -> Thread(runnable, "batch-status-updater").apply { isDaemon = false } }
            return Executors.newSingleThreadExecutor(factory)
        }
    }
}
