package dev.yidafu.flowtrace.ingest.service

import dev.yidafu.flowtrace.ingest.config.UploadAsyncConfig
import dev.yidafu.flowtrace.shared.model.Channel
import dev.yidafu.flowtrace.shared.model.TraceBatchStatus
import dev.yidafu.flowtrace.shared.persistence.repository.TraceBatchRepository
import dev.yidafu.flowtrace.shared.persistence.repository.TraceBatchRow
import kotlinx.coroutines.asCoroutineDispatcher
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * 回归：批记录写入必须真正走异步消费者协程，且队列有界。
 *
 * 守的三个点：
 * 1. `submit` 立即返回（不在请求线程上写库）——原先 `@Async` 被同类自调用绕过代理时，
 *    落库耗时和异常都会算进上报响应；
 * 2. 落库跑在 `trace-batch-writer-` 专用线程上，而不是 `Dispatchers.IO` 共享池；
 * 3. 队列满时 `submit` 返回 `false`（调用方丢弃台账），**绝不阻塞**请求线程。
 */
class TraceBatchWriterAsyncTest {
    @Test
    fun `submit 立即返回且落库在专用协程线程上执行`() {
        val done = CountDownLatch(1)
        var workerThread: String? = null
        // 手写替身而不是 Mockito.any()：Kotlin 非空参数上 matcher 返回 null 会触发 Intrinsics 空指针
        val repository =
            object : TraceBatchRepository() {
                override fun insertIfAbsent(row: TraceBatchRow): Boolean {
                    workerThread = Thread.currentThread().name
                    done.countDown()
                    return true
                }
            }

        withWriter(repository) { writer ->
            val callerThread = Thread.currentThread().name
            val startNs = System.nanoTime()
            val accepted = writer.submit(sampleRow())
            val submitCostMs = (System.nanoTime() - startNs) / 1_000_000

            assertTrue(accepted, "空队列必须收下该行")
            assertTrue(done.await(5, TimeUnit.SECONDS), "消费者未在 5s 内落库")
            assertNotEquals(callerThread, workerThread, "协程调度失效：落库跑在调用方线程上")
            assertTrue(
                workerThread!!.startsWith("trace-batch-writer-"),
                "未使用 UploadAsyncConfig 的专用线程池，实际线程：$workerThread",
            )
            assertTrue(submitCostMs < 1_000, "submit 未立即返回（耗时 ${submitCostMs}ms）")
        }
    }

    @Test
    fun `停机排空把队列里剩余台账写完`() {
        val persisted = AtomicInteger()
        val repository =
            object : TraceBatchRepository() {
                override fun insertIfAbsent(row: TraceBatchRow): Boolean {
                    persisted.incrementAndGet()
                    return true
                }
            }

        withWriter(repository) { writer ->
            repeat(50) { assertTrue(writer.submit(sampleRow()), "队列未满时必须收下") }
            writer.drain()

            assertEquals(50, persisted.get(), "停机前已入队的台账必须写完（否则返回 200 的批次查不到台账）")
        }
    }

    @Test
    fun `队列满时 submit 返回 false 而不是阻塞`() {
        // 卡住全部消费者：此时只有 Channel 的缓冲容量在吸收新行
        val release = CountDownLatch(1)
        val blocked = CountDownLatch(UploadAsyncConfig.PARALLELISM)
        val repository =
            object : TraceBatchRepository() {
                override fun insertIfAbsent(row: TraceBatchRow): Boolean {
                    blocked.countDown()
                    release.await(10, TimeUnit.SECONDS)
                    return true
                }
            }

        withWriter(repository) { writer ->
            // 先用 PARALLELISM 行把消费者全部占住（它们会长住在 insertIfAbsent 里）
            repeat(UploadAsyncConfig.PARALLELISM) { writer.submit(sampleRow()) }
            assertTrue(blocked.await(5, TimeUnit.SECONDS), "消费者没起来")

            var accepted = 0
            var rejected = false
            repeat(UploadAsyncConfig.QUEUE_CAPACITY + 16) {
                if (!rejected) {
                    if (writer.submit(sampleRow())) accepted++ else rejected = true
                }
            }

            assertTrue(rejected, "队列满后 submit 必须返回 false（满了就丢，不能反压主链路）")
            assertEquals(
                UploadAsyncConfig.QUEUE_CAPACITY,
                accepted,
                "消费者全被占住时缓冲应正好收满 capacity 行",
            )
            release.countDown()
        }
    }

    /** 起一个用专用线程池调度器的 writer，用完先排空再关调度器（避免测试进程残留线程）。 */
    private fun withWriter(
        repository: TraceBatchRepository,
        block: (TraceBatchWriter) -> Unit,
    ) {
        val dispatcher = UploadAsyncConfig.traceBatchWriterThreadPool().asCoroutineDispatcher()
        val writer = TraceBatchWriter(repository, dispatcher)
        try {
            block(writer)
        } finally {
            // drain 幂等：队列已关时只是再 join 一次消费者
            writer.drain()
            dispatcher.close()
        }
    }

    private fun sampleRow(): TraceBatchRow {
        val now = Instant.now()
        return TraceBatchRow(
            batchId = UUID.fromString("0190f2a0-0000-7000-8000-000000000001"),
            userId = "u-1",
            deviceId = "dev-1",
            channel = Channel.REALTIME,
            pointCount = 1,
            firstTs = now,
            lastTs = now,
            clientTs = now,
            receivedAt = now,
            sdkVersion = "1.0.0",
            platform = "android",
            status = TraceBatchStatus.ACCEPTED,
        )
    }
}
