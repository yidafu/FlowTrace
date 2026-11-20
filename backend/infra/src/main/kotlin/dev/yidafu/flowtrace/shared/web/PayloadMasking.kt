package dev.yidafu.flowtrace.shared.web

/**
 * 日志载荷脱敏与截断（07 §5：**日志禁止输出坐标明文**）。
 *
 * 请求/响应体落日志前必须过这里：坐标类字段（latQ/lngQ/lat/lng/coordinates 等）替换成 `"***"`，
 * 其余字段（批头、点数、时间戳、错误码）保留——排障需要的是「哪一批、多少点、错在哪」，不是点位。
 */
object PayloadMasking {
    /** 坐标类标量字段：整型差值（latQ/lngQ）与经纬度浮点（lat/lng/latitude/longitude）。 */
    private val COORDINATE_SCALARS =
        Regex("(\"(?:latQ|lngQ|lat|lng|latitude|longitude)\"\\s*:\\s*)-?\\d+(?:\\.\\d+)?", RegexOption.IGNORE_CASE)

    /** GeoJSON 坐标数组（可能嵌套多层），整体替换。 */
    private const val COORDINATES_KEY = "\"coordinates\""

    private const val MASKED = "\"***\""

    /** 脱敏：坐标标量 + 坐标数组。 */
    fun mask(raw: String): String {
        if (raw.isEmpty()) return raw
        val scalarsMasked = COORDINATE_SCALARS.replace(raw) { match -> match.groupValues[1] + MASKED }
        return maskCoordinateArrays(scalarsMasked)
    }

    /** 截断超长载荷，附带原始长度便于判断是否丢了解析上下文。 */
    fun truncate(
        text: String,
        maxChars: Int,
    ): String {
        if (maxChars <= 0 || text.length <= maxChars) return text
        return text.take(maxChars) + "…(truncated, total=${text.length})"
    }

    /** 找到 `"coordinates"` 的数组值，按括号配对整体替换为 `"***"`（跳过字符串字面量内的括号）。 */
    private fun maskCoordinateArrays(text: String): String {
        val out = StringBuilder(text.length)
        var cursor = 0
        while (cursor < text.length) {
            val keyAt = text.indexOf(COORDINATES_KEY, cursor)
            if (keyAt < 0) {
                out.append(text, cursor, text.length)
                break
            }
            val keyEnd = keyAt + COORDINATES_KEY.length
            out.append(text, cursor, keyEnd)
            var arrayAt = keyEnd
            while (arrayAt < text.length && (text[arrayAt].isWhitespace() || text[arrayAt] == ':')) arrayAt++
            if (arrayAt >= text.length || text[arrayAt] != '[') {
                cursor = keyEnd
                continue
            }
            // 原样保留键与数组值之间的冒号/空白，只替换数组本身
            out.append(text, keyEnd, arrayAt)
            out.append(MASKED)
            cursor = matchingBracketEnd(text, arrayAt)
        }
        return out.toString()
    }

    /** 返回与 [openAt] 处 `[` 配对的 `]` 之后的下标；括号不配对时返回文本末尾。 */
    private fun matchingBracketEnd(
        text: String,
        openAt: Int,
    ): Int {
        var depth = 0
        var i = openAt
        while (i < text.length) {
            when (text[i]) {
                '[' -> depth++
                ']' -> {
                    depth--
                    if (depth == 0) return i + 1
                }
                '"' -> {
                    // 跳过字符串，避免把字符串里的括号当成结构括号
                    i++
                    while (i < text.length && text[i] != '"') {
                        if (text[i] == '\\') i++
                        i++
                    }
                }
            }
            i++
        }
        return text.length
    }
}
