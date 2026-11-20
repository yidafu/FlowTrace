package dev.yidafu.flowtrace.process.store

import dev.yidafu.flowtrace.process.config.ProcessAsyncConfig
import dev.yidafu.flowtrace.shared.model.TraceBatchStatus
import dev.yidafu.flowtrace.shared.persistence.repository.BatchStatusChange
import dev.yidafu.flowtrace.shared.persistence.repository.TraceBatchRepository
import kotlinx.coroutines.asCoroutineDispatcher
import org.junit.jupiter.api.AfterEach
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * 台账**成批**回写（06 §3.2 第 8 步；2026-11-20 容量报告 §6）。
 *
 * 背景：逐条 `UPDATE ... WHERE batch_id = ?` 时，处理侧推到 ~500 批/s 就把队列（2048）撑满、
 * 开始丢状态流转。改成「取 1~100 条合成一条 `UPDATE ... WHERE batch_id IN (...)`」后要守住三条：
 * 单次不超过 [ProcessAsyncConfig.STATUS_BATCH_SIZE]、一条状态不丢、停机排空不留尾巴。
 */
class BatchStatusUpdaterTest {
    private val pool =
        Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "batch-status-updater-test").apply { isDaemon = true }
        }
    private val dispatcher = pool.asCoroutineDispatcher()

    @AfterEach
    fun tearDown() {
        dispatcher.close()
    }

    @Test
    fun `250 条状态流转成批回写，单次不超过 100 条且一条不丢`() {
        val repository = RecordingRepository(CountDownLatch(250))
        val updater = BatchStatusUpdater(repository, dispatcher)

        repeat(250) { updater.markProcessed(UUID.randomUUID()) }

        assertTrue(repository.await(5), "5s 内应回写满 250 条")
        val sizes = repository.batches.map { it.size }
        assertTrue(
            sizes.all { it <= ProcessAsyncConfig.STATUS_BATCH_SIZE },
            "单次回写不应超过 ${ProcessAsyncConfig.STATUS_BATCH_SIZE} 条，实际 $sizes",
        )
        val distinctIds = repository.all().map { it.batchId }.toSet()
        assertEquals(250, distinctIds.size)
        assertEquals(0, updater.pending())
        updater.shutdown()
    }

    @Test
    fun `消费者被拖住时，后续状态合并且状态与错误码原样落库`() {
        val repository = GatedRepository()
        val updater = BatchStatusUpdater(repository, dispatcher)

        // 第一条把消费者卡在「DB 慢」里，后续 240 条只能排队 → 放开后应被成批取走
        updater.markProcessed(UUID.randomUUID())
        assertTrue(repository.entered.await(5, TimeUnit.SECONDS), "消费者应先取走第一条并卡住")

        val failed = UUID.randomUUID()
        val dlq = UUID.randomUUID()
        repeat(238) { updater.markProcessed(UUID.randomUUID()) }
        updater.markFailed(failed, "TOMBSTONE")
        updater.markDlq(dlq, "BATCH_TIMEOUT")
        repository.release.countDown()

        awaitCondition("241 条应全部回写") { repository.all().size == 241 }
        val byId = repository.all().associateBy { it.batchId }
        assertEquals(TraceBatchStatus.FAILED, byId.getValue(failed).status)
        assertEquals("TOMBSTONE", byId.getValue(failed).errorCode)
        assertEquals(TraceBatchStatus.DLQ, byId.getValue(dlq).status)
        assertEquals("BATCH_TIMEOUT", byId.getValue(dlq).errorCode)
        assertNull(byId.values.first { it.status == TraceBatchStatus.PROCESSED }.errorCode)

        // 排队期间没有逐条落库，放开后至少有一次取满了整批
        assertTrue(
            repository.batches.any { it.size == ProcessAsyncConfig.STATUS_BATCH_SIZE },
            "排队 240 条后应出现取满 ${ProcessAsyncConfig.STATUS_BATCH_SIZE} 条的回写，实际 ${repository.batches.map { it.size }}",
        )
        updater.shutdown()
    }

    @Test
    fun `停机排空：shutdown 后队列为空且存量已写完`() {
        val repository = RecordingRepository(CountDownLatch(30))
        val updater = BatchStatusUpdater(repository, dispatcher)

        repeat(30) { updater.markProcessed(UUID.randomUUID()) }
        updater.shutdown()

        assertEquals(0, updater.pending())
        assertEquals(30, repository.all().size)
    }

    private fun awaitCondition(
        message: String,
        timeoutMs: Long = 5_000,
        check: () -> Boolean,
    ) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (check()) return
            Thread.sleep(5)
        }
        fail(message)
    }

    /** 记录每次回写的内容；[latch] 在「已回写条数」上倒数，用于等消费者跑完。 */
    private open class RecordingRepository(
        private val latch: CountDownLatch,
    ) : TraceBatchRepository() {
        val batches = CopyOnWriteArrayList<List<BatchStatusChange>>()

        fun all(): List<BatchStatusChange> = batches.flatten()

        fun await(seconds: Long): Boolean = latch.await(seconds, TimeUnit.SECONDS)

        override fun applyStatusBatch(
            changes: List<BatchStatusChange>,
            at: Instant,
        ): Int {
            batches += changes.toList()
            repeat(changes.size) { latch.countDown() }
            return changes.size
        }
    }

    /** 第一次回写阻塞，用来制造「消费者忙、队列积压」的确定场景。 */
    private class GatedRepository : RecordingRepository(CountDownLatch(241)) {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)

        override fun applyStatusBatch(
            changes: List<BatchStatusChange>,
            at: Instant,
        ): Int {
            if (batches.isEmpty()) {
                entered.countDown()
                release.await(5, TimeUnit.SECONDS)
            }
            return super.applyStatusBatch(changes, at)
        }
    }
}
