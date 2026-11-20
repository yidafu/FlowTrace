package dev.yidafu.flowtrace.ingest.config

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

/**
 * 上报侧批记录落库的协程调度器（06 §2.2 第 9 步：`INSERT trace_batch` 走「有界 Channel → 消费者协程」）。
 *
 * 为什么还要显式定义：协程不是「免线程池」。`Dispatchers.IO` 是全站共享的 64 线程池，
 * 直接 `scope.launch { 同步 JDBC }` 在 PG 抖动时会把 query / admin 的 IO 也一起挤掉。
 * 这里给一条**专用**的固定 [PARALLELISM] 线程调度器并命名线程，阻塞点（JDBC）只在这几个线程上排队。
 *
 * 有界性不在线程池，而在 [dev.yidafu.flowtrace.ingest.service.TraceBatchWriter] 的 `Channel(capacity = QUEUE_CAPACITY)`：
 * `trySend` 满了立即返回失败、调用方丢弃该条台账，**绝不在请求线程上排队或写库**——批记录是旁路数据，
 * 不能反压主上报链路（06 §2.2 第 9 步）。
 *
 * 停机不靠 `awaitTermination`：writer 的 `@PreDestroy` 先 `Channel.close()` 排空存量，
 * 再等 [DRAIN_TIMEOUT_MS]，最后关这条调度器（Spring 按依赖顺序先销毁 writer 再销毁本 bean）。
 */
@Configuration
class UploadAsyncConfig {
    /**
     * 批记录落库的专用调度器。bean 名必须与 [WRITER_DISPATCHER] 一致——writer 按名注入，
     * 改名即断链（`@Qualifier` 解析失败会在启动期直接报错，比运行期静默降级好）。
     */
    @Bean(WRITER_DISPATCHER, destroyMethod = "close")
    fun traceBatchWriterDispatcher(): CoroutineDispatcher = traceBatchWriterThreadPool().asCoroutineDispatcher()

    companion object {
        /** writer 注入调度器用的 bean 名；改名即断链。 */
        const val WRITER_DISPATCHER = "traceBatchWriterDispatcher"

        /** 有界队列容量：满即丢弃并告警（沿用旧 `ThreadPoolTaskExecutor` 时代的 2048）。 */
        const val QUEUE_CAPACITY = 2048

        /** 并发消费者数 = 专用线程数，避免「4 个线程、100 个协程」的假并发。 */
        const val PARALLELISM = 4

        /** 停机排空上限：正常重启时把队列里已返回 200 的台账写完。 */
        const val DRAIN_TIMEOUT_MS = 5_000L

        /** 专用线程池：线程名带前缀，堆栈/线程 dump 里能一眼认出是批记录落库。 */
        fun traceBatchWriterThreadPool(): ExecutorService {
            val seq = AtomicInteger()
            val factory =
                ThreadFactory { runnable ->
                    Thread(runnable, "trace-batch-writer-${seq.incrementAndGet()}").apply { isDaemon = false }
                }
            return Executors.newFixedThreadPool(PARALLELISM, factory)
        }
    }
}
