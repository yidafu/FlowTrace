package dev.yidafu.flowtrace.query.web

import dev.yidafu.flowtrace.shared.persistence.repository.TraceSegmentCursor
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class CursorCodecTest {
    private val codec = CursorCodec()

    @Test
    fun `游标往返保持毫秒时间戳与 UUID 且不带 padding`() {
        val cursor = TraceSegmentCursor(1_763_376_000_000L, UUID.fromString("9af0c1d2-0000-5000-8000-000000000001"))

        val encoded = codec.encode(cursor)

        assertFalse(encoded.contains('='), "base64url 不应带 padding")
        assertEquals(cursor, codec.decode(encoded, TraceSegmentCursor::class.java))
    }

    @Test
    fun `非法游标抛 InvalidCursorException 以便控制器转 400`() {
        assertFailsWith<InvalidCursorException> {
            codec.decode("not-a-cursor", TraceSegmentCursor::class.java)
        }
    }
}
