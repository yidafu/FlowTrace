package dev.yidafu.flowtrace.shared.protocol

import dev.yidafu.flowtrace.v1.GeoBase
import dev.yidafu.flowtrace.v1.TraceBatchHeader
import dev.yidafu.flowtrace.v1.TraceBatchRequest
import dev.yidafu.flowtrace.v1.TracePointDelta
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * 协议字节数守护（02 §2 / §4）。
 *
 * 存在的理由：`proto/trace.proto` 的增量字段曾写成 `int32`（设计稿是 `sint64`），
 * 而 int32 的负值会被符号扩展到 64 位、编码成 **10 字节** varint —— 轨迹增量有一半概率为负，
 * 等于每点白送 8~16 字节，且**编译、解码、往返测试全部通过**，没有任何迹象。
 * 200 点批实测：int32 8169B → sint64 5736B（未压缩）。
 *
 * 所以这里钉的是**字节数**和**固定 wire fixture**，而不是「解出来对不对」：
 * 只要有人把 sint 改回 int32/int64，或者字段编号挪了位置，这些用例立刻红。
 * fixture 同时充当跨语言契约（TS / Kotlin / Python 必须解出同一组值）。
 */
class DeltaEncodingSizeTest {
    @Test
    fun `负增量走 zigzag 单点 10 字节`() {
        val point = delta(20_000, -2_335, 2_335)

        // d_ts 4B（tag + 3B varint）+ d_lat 3B + d_lng 3B；写成 int32 时 d_lat 会膨胀到 11B
        assertEquals(10, point.serializedSize, "负增量若占 10 字节 varint，说明字段类型退回了 int32/int64")
    }

    @Test
    fun `负坐标基准值 19 字节`() {
        // 南半球 + 西经：lat_q/lng_q 均为负，zigzag 后各 5 字节（int32 下各 10 字节）
        val base =
            GeoBase
                .newBuilder()
                .setTsMs(1_795_000_000_000L)
                .setLatQ(-338_600_000L)
                .setLngQ(-701_200_000L)
                .build()

        assertEquals(19, base.serializedSize)
    }

    @Test
    fun `固定 wire fixture 解出的增量符号正确`() {
        val point = TracePointDelta.parseFrom(hex("08 c0 b8 02 10 bd 24 18 be 24"))

        assertEquals(20_000L, point.dTs)
        assertEquals(-2_335L, point.dLat, "zigzag 未解码时这里会得到 4_669，坐标直接反向")
        assertEquals(2_335L, point.dLng)
    }

    @Test
    fun `批内每点固定开销 12 字节`() {
        // 单点 10B + repeated 字段的 tag/长度 2B
        val one = request(points = 1).serializedSize
        val many = request(points = 200).serializedSize

        assertEquals(12, (many - one) / 199)
    }

    private fun request(points: Int): TraceBatchRequest =
        TraceBatchRequest
            .newBuilder()
            .setHeader(
                TraceBatchHeader
                    .newBuilder()
                    .setBatchId("0190f2a0-0000-7000-8000-0000000000aa")
                    .setSchemaVersion("1.0")
                    .setDeviceId("dev-1")
                    .setUserId("u-1")
                    .setBase(
                        GeoBase
                            .newBuilder()
                            .setTsMs(1_795_000_000_000L)
                            .setLatQ(390_000_000L)
                            .setLngQ(1_160_000_000L),
                    ),
            ).addAllPoints(List(points) { delta(20_000, -2_335, 2_335) })
            .build()

    private fun delta(
        dTs: Long,
        dLat: Long,
        dLng: Long,
    ): TracePointDelta =
        TracePointDelta
            .newBuilder()
            .setDTs(dTs)
            .setDLat(dLat)
            .setDLng(dLng)
            .build()

    private fun hex(text: String): ByteArray =
        text
            .split(" ")
            .map { it.toInt(16).toByte() }
            .toByteArray()
}
