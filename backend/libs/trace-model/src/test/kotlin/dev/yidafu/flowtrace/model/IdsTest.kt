package dev.yidafu.flowtrace.model

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class IdsTest {
    /** RFC 9562 / RFC 4122 附录的 DNS 命名空间标准向量，用来证明实现不是 MD5(v3) 也不是自造算法。 */
    @Test
    fun `v5 命中 RFC 标准向量`() {
        val dns = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        assertEquals(UUID.fromString("2ed6657d-e927-568b-95e1-2665a8aea6a2"), Ids.v5(dns, "www.example.com"))
    }

    @Test
    fun `v5 版本位与变体位符合规范`() {
        val id = Ids.v5(Ids.POINT_NS, "x")
        assertEquals(5, id.version())
        assertEquals(2, id.variant())
        // 反向断言：Java 自带的是 v3（MD5），两者绝不能相等，否则说明误用了 nameUUIDFromBytes
        assertNotEquals(UUID.nameUUIDFromBytes("x".toByteArray()), id)
    }

    /** 与 Python `uuid.uuid5` 独立实现的交叉验证（跨语言同值，02 §7 双端口径）。 */
    @Test
    fun `pointId 与 Python 参考实现一致`() {
        val expected = UUID.fromString("6d8f10b7-2225-5a6e-9841-0114651d9fb4")
        assertEquals(expected, Ids.pointId("018f2b3c-0000-7000-8000-000000000001", 0))
    }

    @Test
    fun `pointId 由 batchId 字符串与 idx 唯一确定`() {
        val a = Ids.pointId("018f2b3c-0000-7000-8000-000000000001", 0)
        val b = Ids.pointId("018f2b3c-0000-7000-8000-000000000001", 0)
        val c = Ids.pointId("018f2b3c-0000-7000-8000-000000000001", 1)
        val d = Ids.pointId("018F2B3C-0000-7000-8000-000000000001", 0)
        assertEquals(a, b)
        assertNotEquals(a, c)
        // batchId 按原样字符串参与派生：大小写不同即不同 id（跨端必须统一成一种文本形式）
        assertNotEquals(a, d)
    }

    @Test
    fun `segmentId 与 Python 参考实现一致`() {
        val expected = UUID.fromString("44bac973-0cae-50b1-aebe-e635f4252f65")
        assertEquals(expected, Ids.segmentId("u-1001", "s-1", 1_700_000_000_000L))
    }

    @Test
    fun `segmentId 对同一段起点稳定 - 因此可 upsert 而非新增`() {
        val first = Ids.segmentId("u-1001", "s-1", 1_700_000_000_000L)
        val again = Ids.segmentId("u-1001", "s-1", 1_700_000_000_000L)
        val extended = Ids.segmentId("u-1001", "s-1", 1_700_000_001_000L)
        assertEquals(first, again)
        assertNotEquals(first, extended)
    }

    @Test
    fun `sessionId 为空与空串等价`() {
        assertEquals(
            Ids.segmentId("u-1001", null, 1_700_000_000_000L),
            Ids.segmentId("u-1001", "", 1_700_000_000_000L),
        )
        assertEquals(
            UUID.fromString("84937970-9b11-5eb0-a89c-d8c9247ee9dd"),
            Ids.segmentId("u-1001", null, 1_700_000_000_000L),
        )
    }
}
