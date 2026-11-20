package dev.yidafu.flowtrace.shared.web

import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * 日志载荷脱敏（07 §5 日志不出坐标）。
 */
class PayloadMaskingTest {
    @Test
    fun `坐标标量与坐标数组都被脱敏，其余字段保留`() {
        val raw =
            """{"header":{"latQ":390000000,"lngQ":1160000000,"tsMs":1000},""" +
                """"geometry":{"coordinates":[[116.1234567,39.1234567],[116.2,39.2]]},"points":3}"""

        val masked = PayloadMasking.mask(raw)

        assertFalse(masked.contains("390000000"), "latQ 明文不应出现：$masked")
        assertFalse(masked.contains("116.1234567"), "coordinates 明文不应出现：$masked")
        assertContains(masked, "\"latQ\":\"***\"")
        assertContains(masked, "\"coordinates\":\"***\"")
        // 非坐标字段保持原样，排障靠它们
        assertContains(masked, "\"tsMs\":1000")
        assertContains(masked, "\"points\":3")
    }

    @Test
    fun `带空格的经纬度浮点同样被脱敏且保留原有分隔`() {
        val masked = PayloadMasking.mask("""{"lat": 39.1234567, "lng": 116.9876543}""")

        assertFalse(masked.contains("39.1234567"))
        assertFalse(masked.contains("116.9876543"))
        assertEquals("""{"lat": "***", "lng": "***"}""", masked)
    }

    @Test
    fun `字符串里的方括号不影响坐标数组配对`() {
        val raw = """{"note":"[不是数组]","coordinates":[[1,2],[3,4]],"tail":true}"""

        val masked = PayloadMasking.mask(raw)

        assertContains(masked, """{"note":"[不是数组]","coordinates":"***","tail":true}""")
    }

    @Test
    fun `超长载荷截断并标注原始长度`() {
        val text = "a".repeat(100)

        val truncated = PayloadMasking.truncate(text, 10)

        assertTrue(truncated.startsWith("a".repeat(10)))
        assertContains(truncated, "total=100")
        assertEquals(text, PayloadMasking.truncate(text, 1000))
    }
}
