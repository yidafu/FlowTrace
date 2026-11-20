package dev.yidafu.flowtrace.shared.model

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * 标识派生（02 §7 / 决策 D3、D5、D8）。
 *
 * [POINT_NS] / [SEGMENT_NS] 是**跨端同值**的固定命名空间常量：SDK、服务端、存储三处
 * 若各自换一套，pointId/segmentId 就对不上，幂等与重算全部失效。改动值等于改协议，
 * 必须同步 `proto/trace.proto` 头部注释与 golden vector。
 */
object Ids {
    /** pointId 命名空间：`UUIDv5(POINT_NS, "{batchId}:{idx}")`。 */
    val POINT_NS: UUID = UUID.fromString("7d1a6b34-8f2c-4e5a-9b0d-1c2e3f4a5b6c")

    /** segmentId 命名空间：`UUIDv5(SEGMENT_NS, "{userId}:{sessionId}:{startTsMs}")`。 */
    val SEGMENT_NS: UUID = UUID.fromString("3e9f2a7c-6d4b-4c8e-8a1f-2b3c4d5e6f7a")

    /**
     * SHA-1 实例按线程复用。
     *
     * `MessageDigest.getInstance("SHA-1")` 走的是服务提供者查找，单次开销比「摘要 16 字节」本身还大；
     * 而点级派生是**每点一次**（10 亿点/天 ≈ 1.2 万次/秒）。`MessageDigest` 不是线程安全的，
     * 所以用 ThreadLocal 而不是共享单例：线程数由容器/消费并发决定，有界。
     */
    private val SHA1: ThreadLocal<MessageDigest> = ThreadLocal.withInitial { MessageDigest.getInstance("SHA-1") }

    /**
     * 命名空间 → 16 字节前缀（`mostSignificantBits || leastSignificantBits`）。
     *
     * 命名空间只有 [POINT_NS] / [SEGMENT_NS] 两个固定值，按 UUID 缓存后不必每次
     * 分配 `ByteBuffer` 再拼一遍；容量只是防御第三方传入其它命名空间。
     */
    private val NAMESPACE_PREFIX = ConcurrentHashMap<UUID, ByteArray>(4)

    /**
     * UUIDv5（RFC 9562 §5.5）：`SHA-1(namespace 字节 || name UTF-8)` 取前 16 字节，
     * 再按规范置版本位（第 7 字节高 4 位 = 0101）与变体位（第 9 字节高 2 位 = 10）。
     *
     * **不要用 `UUID.nameUUIDFromBytes`**：那是 v3（MD5），值不同且不可跨语言复现
     * （端侧 protobuf-es / TS 生态默认就是 v5）。这里也不使用 `ByteBuffer` 之外的工具，
     * 保证与 JS、Kotlin、PG 侧算法逐字节一致。
     */
    fun v5(
        namespace: UUID,
        name: String,
    ): UUID {
        val digest = SHA1.get()
        // reset 是必需的：线程上次可能在中途抛过异常，残留的摘要状态会污染本次结果
        digest.reset()
        digest.update(NAMESPACE_PREFIX.computeIfAbsent(namespace) { it.toNamespaceBytes() })
        val hash = digest.digest(name.toByteArray(StandardCharsets.UTF_8))
        hash[6] = ((hash[6].toInt() and 0x0F) or 0x50).toByte()
        hash[8] = ((hash[8].toInt() and 0x3F) or 0x80).toByte()
        val buffer = ByteBuffer.wrap(hash, 0, 16)
        return UUID(buffer.long, buffer.long)
    }

    /** 命名空间的 16 字节大端表示（与 RFC 4122 §4.3 的 `namespace || name` 拼接一致）。 */
    private fun UUID.toNamespaceBytes(): ByteArray =
        ByteBuffer
            .allocate(16)
            .putLong(mostSignificantBits)
            .putLong(leastSignificantBits)
            .array()

    /**
     * 点级幂等键：`UUIDv5(POINT_NS, "{batchId}:{idx}")`。
     *
     * [batchId] 用**原样字符串**（SDK 生成的 UUIDv7 文本形式），不要先解析成 UUID 再 `toString()`——
     * 两者在大小写/连字符风格不一致时会派生不同 id，导致同一物理点被写两次。
     * [idx] 是批内序号（0 起），批一旦成型成员与顺序不再变（02 §7 不变式 3）。
     */
    fun pointId(
        batchId: String,
        idx: Int,
    ): UUID = v5(POINT_NS, "$batchId:$idx")

    /**
     * 段标识：`UUIDv5(SEGMENT_NS, "{userId}:{sessionId}:{startTsMs}")`。
     *
     * 确定性派生 ⇒ 段被后续点延长时 id 不变 ⇒ upsert 更新而非新增（D8）。
     *
     * 已知边界：`sessionId` 为空时，同用户同毫秒在两台设备上启动的段会得到同一 id
     * （03 §3 允许 `session_id` 为 NULL）。`docs/design/08-processor-impl.md` §6.2 建议写入侧
     * 删除范围额外按 `device_id` 收敛；是否把 `deviceId` 纳入派生输入需协议侧确认，
     * **未确认前不要擅自改本函数**。
     */
    fun segmentId(
        userId: String,
        sessionId: String?,
        startTsMs: Long,
    ): UUID = v5(SEGMENT_NS, "$userId:${sessionId.orEmpty()}:$startTsMs")
}
