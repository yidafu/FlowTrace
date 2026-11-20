package dev.yidafu.flowtrace.ingest.service

import dev.yidafu.flowtrace.ingest.config.UploadAsyncConfig
import dev.yidafu.flowtrace.shared.persistence.repository.TraceBatchRepository
import dev.yidafu.flowtrace.shared.persistence.repository.TraceBatchRow
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component

/**
 * 批记录异步落库（06 §2.2 第 9 步，`INSERT trace_batch ACCEPTED`）。
 *
 * 模型：**有界 Channel + [UploadAsyncConfig.PARALLELISM] 个消费者协程**，替代原先的 `@Async` + `ThreadPoolTaskExecutor`：
 * - [submit] 只做 `trySend`，**不阻塞请求线程、不写库**；队列满或正在停机时返回 `false`，调用方打 ERROR 并丢弃
 *   （批记录是旁路数据，不能反压主链路，Kafka 此时已确认）；
 * - 消费者从同一个 Channel 取行，Channel 天然支持多消费者公平分发，等价于「小线程池 + 有界队列」；
 * - 落库异常不能抛回请求线程，只记 ERROR（带 batchId，便于事后按批补录）。
 *
 * 停机（[@PreDestroy]）：先 `close()` 不收新行，再等最多 [UploadAsyncConfig.DRAIN_TIMEOUT_MS] 把存量写完，
 * 避免正常重启丢掉已经返回 200 的台账。
 */
@Component
class TraceBatchWriter(
    private val batchRepository: TraceBatchRepository,
    @Qualifier(UploadAsyncConfig.WRITER_DISPATCHER) dispatcher: CoroutineDispatcher,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /** 有界队列：满即 `trySend` 失败，由调用方丢弃并告警（不阻塞、不反压主链路）。 */
    private val queue = Channel<TraceBatchRow>(capacity = UploadAsyncConfig.QUEUE_CAPACITY)

    private val scope = CoroutineScope(SupervisorJob() + dispatcher + CoroutineName("trace-batch-writer"))

    /**
     * 消费者协程：个数与专用线程数一致。用 `SupervisorJob` 兜底——单个消费者意外退出不影响其余消费者，
     * 且 [drainQueue] 内部已把逐行异常吞掉，正常不会退出。
     */
    private val consumers: List<Job> = List(UploadAsyncConfig.PARALLELISM) { scope.launch { drainQueue() } }

    /**
     * 投递一条批次台账（`trace_batch`，status 由调用方给 ACCEPTED）。
     *
     * 返回 `false` 表示队列已满或已停机（此时该行被丢弃），调用方负责带 batchId 打 ERROR。
     * 返回 `true` 只代表**入队成功**：插入是否真的成功只能看日志（`inserted=false` 表示 batchId 已存在，
     * 由 `insertIfAbsent` 幂等吞掉）。
     */
    fun submit(row: TraceBatchRow): Boolean = queue.trySend(row).isSuccess

    /** 消费者主循环：Channel 关闭且排空后 `for-in` 自然结束。 */
    private suspend fun drainQueue() {
        for (row in queue) {
            persist(row)
        }
    }

    private fun persist(row: TraceBatchRow) {
        try {
            val inserted = batchRepository.insertIfAbsent(row)
            log.debug(
                "upload 批次记录落库完成 batchId={} deviceId={} inserted={}",
                row.batchId,
                row.deviceId,
                inserted,
            )
        } catch (ex: CancellationException) {
            // 停机取消：必须原样抛出，否则取消会被当成普通异常吞掉、消费者无法退出
            throw ex
        } catch (ex: Throwable) {
            // Kafka 已写入、响应已返回 200：这里丢的是「批次台账」一行，靠 batchId 唯一索引重放可补齐
            log.error(
                "upload 批次记录落库失败（Kafka 已写入，台账缺失，重放依赖幂等） batchId={} deviceId={} points={} cause={}",
                row.batchId,
                row.deviceId,
                row.pointCount,
                ex.message,
                ex,
            )
        }
    }

    /** 停机排空：关闭队列 → 等存量写完（上限 5s）→ 取消协程作用域。 */
    @PreDestroy
    fun drain() {
        queue.close()
        val drained =
            runBlocking {
                withTimeoutOrNull(UploadAsyncConfig.DRAIN_TIMEOUT_MS) {
                    consumers.joinAll()
                    true
                }
            }
        if (drained != true) {
            log.warn(
                "upload 批记录队列未在 {}ms 内排空，剩余台账丢弃（重启后可按 batchId 重放补齐）",
                UploadAsyncConfig.DRAIN_TIMEOUT_MS,
            )
        }
        scope.cancel()
    }
}
