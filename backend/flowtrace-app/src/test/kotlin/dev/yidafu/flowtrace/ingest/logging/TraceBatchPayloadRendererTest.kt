package dev.yidafu.flowtrace.ingest.logging

import dev.yidafu.flowtrace.v1.Channel
import dev.yidafu.flowtrace.v1.GeoBase
import dev.yidafu.flowtrace.v1.SdkInfo
import dev.yidafu.flowtrace.v1.TraceBatchHeader
import dev.yidafu.flowtrace.v1.TraceBatchRequest
import dev.yidafu.flowtrace.v1.TracePointDelta
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * 上报请求体日志摘要测试（07 §5）。
 *
 * 两条硬约束都不能靠 code review 保证，必须有用例兜住：
 * - **日志不出坐标**：坐标为敏感位置数据，摘要里只允许出现 batchId/deviceId/计数/时间戳；
 * - 解析失败也必须给出兜底摘要（字节数），否则「解析不了的请求」在日志里完全没有痕迹。
 */
class TraceBatchPayloadRendererTest {
    private val renderer = TraceBatchPayloadRenderer()

    @Test
    fun `只认 protobuf 内容类型`() {
        assertTrue(renderer.supports("application/x-protobuf"))
        assertTrue(renderer.supports("application/protobuf;charset=UTF-8"))
        assertTrue(renderer.supports("APPLICATION/X-PROTOBUF"), "Content-Type 判定必须大小写不敏感")
        assertFalse(renderer.supports("application/json"))
        assertFalse(renderer.supports(null))
    }

    @Test
    fun `摘要带批次标识与计数但不含任何坐标`() {
        val text = renderer.render("application/x-protobuf", request(listOf(0L, 1_000L, 2_000L)).toByteArray())

        assertContains(text, "batchId=$BATCH_ID")
        assertContains(text, "deviceId=dev-1")
        assertContains(text, "userId=u-1")
        assertContains(text, "channel=CHANNEL_REALTIME")
        assertContains(text, "sdk=android/1.0.0")
        assertContains(text, "points=3")
        // base ts 1000 + 链式增量 0/1000/2000 → 1000, 2000, 4000
        assertContains(text, "tsRangeMs=1000..4000")
        assertContains(text, "baseTsMs=1000")
        assertContains(text, "encodedBytes=")

        // 隐私：base 坐标与任何点都不能落到日志里
        assertFalse(text.contains("390000000"), "日志泄露了纬度量化值：$text")
        assertFalse(text.contains("1160000000"), "日志泄露了经度量化值：$text")
        assertFalse(text.contains("39.0"), "日志泄露了纬度：$text")
        assertFalse(text.contains("116."), "日志泄露了经度：$text")
    }

    @Test
    fun `空批的时间区间记为占位符`() {
        val text = renderer.render("application/x-protobuf", request(emptyList()).toByteArray())

        assertContains(text, "tsRangeMs=-")
        assertContains(text, "points=0")
    }

    @Test
    fun `无法解析的字节给出兜底摘要并带字节数`() {
        val garbage = byteArrayOf(0xFF.toByte(), 0xFF.toByte(), 0xFF.toByte(), 0xFF.toByte())

        val text = renderer.render("application/x-protobuf", garbage)

        assertContains(text, "protobuf 无法解析")
        assertContains(text, "4B")
    }

    @Test
    fun `未指明通道按协议缺省输出`() {
        val batch =
            request(listOf(0L))
                .toBuilder()
                .setHeader(request(listOf(0L)).header.toBuilder().setChannel(Channel.CHANNEL_UNSPECIFIED))
                .build()

        assertContains(renderer.render("application/x-protobuf", batch.toByteArray()), "channel=CHANNEL_UNSPECIFIED")
    }

    private fun request(tsDeltas: List<Long>): TraceBatchRequest =
        TraceBatchRequest
            .newBuilder()
            .setHeader(
                TraceBatchHeader
                    .newBuilder()
                    .setBatchId(BATCH_ID)
                    .setSchemaVersion("1.0")
                    .setDeviceId("dev-1")
                    .setUserId("u-1")
                    .setChannel(Channel.CHANNEL_REALTIME)
                    .setBase(
                        GeoBase
                            .newBuilder()
                            .setTsMs(1_000L)
                            .setLatQ(390000000)
                            .setLngQ(1160000000),
                    ).setSdk(
                        SdkInfo
                            .newBuilder()
                            .setSdkVersion("1.0.0")
                            .setPlatform("android")
                            .setAppVersion("1.2.3"),
                    ),
            ).addAllPoints(tsDeltas.map { TracePointDelta.newBuilder().setDTs(it).build() })
            .build()

    private companion object {
        const val BATCH_ID = "0190f2a0-0000-7000-8000-000000000001"
    }
}
