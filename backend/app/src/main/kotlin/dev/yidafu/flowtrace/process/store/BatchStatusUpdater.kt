package dev.yidafu.flowtrace.process.store

import dev.yidafu.flowtrace.process.config.ProcessAsyncConfig
import dev.yidafu.flowtrace.shared.persistence.repository.TraceBatchRepository
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/**
 * `trace_batch` 状态更新（06 §3.2 第 8 步，08 §6.3）。
 *
 * 为什么必须异步缓冲：`UPDATE trace_batch ... WHERE batch_id = ?` 会扫全部分区
 * （唯一索引落在各子表上），单条耗时远高于写点/写段。这是**兜底审计路径，不是热路径**，
 * 让它挡在消费线程前面等于用一张对账表的写延迟去限制轨迹处理吞吐。
 *
 * 实现是「有界 Channel + 单消费者协程」（与 upload 的 `TraceBatchWriter` 同构），
 * 取代原先手写的 worker 线程——线程启动不再发生在构造函数里（`this` 不会在构造期逸出），
 * 停机也走同一条 `close → 排空 → cancel` 路径：
 * - [markProcessed] 等只做 `trySend`，**不阻塞消费线程、不写库**；队列满或已停机立即失败，
 *   打 ERROR 后丢弃 —— 丢的只是一次状态流转，台账会停在 ACCEPTED，可按 batchId 重放补齐，
 *   绝不反压消费线程、更不能因此不 ack（与 upload 的批记录写入口径一致）；
 * - **单消费者**：状态更新本身可乱序（不同 batch 互不相关），借它把「按 batchId 扫分区」
 *   这类重查询的并发度钉死在 1，避免拖垮 DB；
 * - 消费者跑在**专用单线程调度器**上（[ProcessAsyncConfig.STATUS_DISPATCHER]），
 *   同步 JDBC 的阻塞只落在这条线程，不占用 `Dispatchers.IO`。
 */
@Component
class BatchStatusUpdater(
    private val repository: TraceBatchRepository,
    @Qualifier(ProcessAsyncConfig.STATUS_DISPATCHER) dispatcher: CoroutineDispatcher,
) : BatchStatusSink {
    private val log = LoggerFactory.getLogger(javaClass)

    /** 有界队列：满即 `trySend` 失败，由 [enqueue] 丢弃并告警（不阻塞、不反压消费线程）。 */
    private val queue = Channel<Command>(capacity = ProcessAsyncConfig.QUEUE_CAPACITY)

    private val scope = CoroutineScope(SupervisorJob() + dispatcher + CoroutineName("batch-status-updater"))

    /** 唯一消费者；[apply] 内已吞掉逐条异常，正常不会退出。 */
    private val consumer: Job = scope.launch { drain() }

    /** 已入队未处理条数：仅用于排查与停机日志（`Channel` 不暴露 size）。 */
    private val pending = AtomicInteger()

    /** 标记处理成功（正常批、迟到批、重算锁让路批都算「已处理」，点已落库）。 */
    override fun markProcessed(batchId: UUID) = enqueue(Command.Processed(batchId))

    /** 标记确定性失败（当前只有墓碑丢弃 `TOMBSTONE`）。 */
    override fun markFailed(
        batchId: UUID,
        errorCode: String,
    ) = enqueue(Command.Failed(batchId, errorCode))

    /** 标记进入死信（06 §3.7）。 */
    override fun markDlq(
        batchId: UUID,
        errorCode: String,
    ) = enqueue(Command.Dlq(batchId, errorCode))

    private fun enqueue(command: Command) {
        if (queue.trySend(command).isFailure) {
            log.error(
                "批状态更新队列已满或已停机，丢弃一次状态流转 batchId={} action={}（台账可后续按 batchId 重放补齐）",
                command.batchId,
                command.action,
            )
            return
        }
        pending.incrementAndGet()
    }

    /** 消费者主循环：Channel 关闭且排空后 `for-in` 自然结束。 */
    private suspend fun drain() {
        for (command in queue) {
            try {
                apply(command)
            } finally {
                pending.decrementAndGet()
            }
        }
    }

    private fun apply(command: Command) {
        try {
            when (command) {
                is Command.Processed -> repository.markProcessed(command.batchId)
                is Command.Failed -> repository.markFailed(command.batchId, command.errorCode)
                is Command.Dlq -> repository.markDlq(command.batchId, command.errorCode)
            }
        } catch (ex: CancellationException) {
            // 停机取消：必须原样抛出，否则取消会被当成普通异常吞掉、消费者无法退出
            throw ex
        } catch (ex: Throwable) {
            // 更新 0 行不是错误（upload 台账可能异步滞后/被丢），真正的异常只记日志：
            // 这里抛出去没人接，状态更新失败不该影响已经 ack 的批次
            log.error(
                "批状态更新失败 batchId={} action={} cause={}",
                command.batchId,
                command.action,
                ex.message,
                ex,
            )
        }
    }

    /** 停机排空：关闭队列 → 等存量写完（上限 5s）→ 取消作用域。 */
    @PreDestroy
    fun shutdown() {
        queue.close()
        val drained =
            runBlocking {
                withTimeoutOrNull(ProcessAsyncConfig.DRAIN_TIMEOUT_MS) {
                    consumer.join()
                    true
                }
            }
        if (drained != true) {
            log.warn("批状态更新停机排空超时，放弃剩余 {} 条（台账可后续按 batchId 重放补齐）", pending.get())
        }
        scope.cancel()
    }

    /** 队列里还排着的条数（测试与排查用）。 */
    fun pending(): Int = pending.get()

    private sealed interface Command {
        val batchId: UUID
        val action: String

        data class Processed(
            override val batchId: UUID,
        ) : Command {
            override val action: String get() = "PROCESSED"
        }

        data class Failed(
            override val batchId: UUID,
            val errorCode: String,
        ) : Command {
            override val action: String get() = "FAILED"
        }

        data class Dlq(
            override val batchId: UUID,
            val errorCode: String,
        ) : Command {
            override val action: String get() = "DLQ"
        }
    }
}
