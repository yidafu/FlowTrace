package dev.yidafu.flowtrace.shared.protocol

import dev.yidafu.flowtrace.shared.protocol.delta.DeltaBatchDecoder
import dev.yidafu.flowtrace.shared.protocol.validation.BatchValidator
import dev.yidafu.flowtrace.shared.protocol.validation.ValidationCode
import dev.yidafu.flowtrace.v1.Channel
import dev.yidafu.flowtrace.v1.GeoBase
import dev.yidafu.flowtrace.v1.SdkInfo
import dev.yidafu.flowtrace.v1.TraceBatchHeader
import dev.yidafu.flowtrace.v1.TraceBatchRequest
import dev.yidafu.flowtrace.v1.TracePointDelta
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Delta 解码 + 字段校验（06 §1.2 / §2.3，02 §9）。
 *
 * 这两支是「脏数据进 Kafka」的最后一道闸：规则漏一条，坏点就会一路流到 processor
 * 的清洗阶段（清洗只处理噪声，不处理非法坐标/越窗时间）。因此每条规则都单独钉一个用例。
 */
class DeltaBatchDecoderTest {
    private val decoder = DeltaBatchDecoder()
    private val validator = BatchValidator()
    private val now = 1_795_000_000_000L

    @Test
    fun `链式增量逐点累加且保留批内 idx`() {
        val decoded = decoder.decode(request(points = listOf(delta(0, 0, 0), delta(1_000, 10, 20), delta(500, -5, 30))))

        assertEquals(3, decoded.rawCount)
        assertEquals(listOf(0, 1, 2), decoded.points.map { it.idx })
        assertEquals(listOf(BASE_TS, BASE_TS + 1_000, BASE_TS + 1_500), decoded.points.map { it.tsMs })
        assertEquals(listOf(390_000_000, 390_000_010, 390_000_005), decoded.points.map { it.latQ })
        assertEquals(listOf(1_160_000_000, 1_160_000_020, 1_160_000_050), decoded.points.map { it.lngQ })
    }

    @Test
    fun `相邻完全重复的点被折叠且保留首点 idx`() {
        val decoded =
            decoder.decode(
                request(
                    points =
                        listOf(
                            delta(0, 0, 0),
                            delta(0, 0, 0), // 与上一点完全相同 → 折叠
                            delta(1_000, 0, 0),
                            delta(0, 0, 0), // 与上一点完全相同 → 折叠
                        ),
                ),
            )

        val folded = decoded.foldedPoints()

        assertEquals(4, decoded.rawCount, "rawCount 是折叠前的点数，用于点数上限校验")
        assertEquals(2, folded.size)
        assertEquals(listOf(0, 2), folded.map { it.idx }, "折叠后下标必须仍是原始 idx，否则 pointId 会漂移")
    }

    @Test
    fun `非相邻的重复位置不折叠`() {
        // A → B → A 是真实往返轨迹，不是静止重复
        val decoded = decoder.decode(request(points = listOf(delta(0, 0, 0), delta(1_000, 10, 0), delta(1_000, -10, 0))))

        assertEquals(3, decoded.foldedPoints().size)
    }

    @Test
    fun `极端增量不回绕成假合法坐标`() {
        val decoded =
            decoder.decode(
                request(points = listOf(delta(0, Int.MAX_VALUE, Int.MAX_VALUE), delta(0, Int.MAX_VALUE, Int.MAX_VALUE))),
            )

        assertTrue(decoded.points[1].latQ == Int.MAX_VALUE, "越界应钳到 int 边界，交给校验器按 INVALID_COORD 拒收")
        val failure = validator.validate(decoded, now, configFor())
        assertEquals(ValidationCode.INVALID_COORD, failure?.code)
    }

    @Test
    fun `极端负增量同样不回绕成假合法坐标`() {
        // 累加落到 Int.MIN_VALUE 时，abs(Int.MIN_VALUE) 仍是负数，用 abs() 比较会放行
        val decoded =
            decoder.decode(
                request(points = listOf(delta(0, Int.MIN_VALUE, Int.MIN_VALUE), delta(0, Int.MIN_VALUE, Int.MIN_VALUE))),
            )

        assertEquals(ValidationCode.INVALID_COORD, validator.validate(decoded, now, configFor())?.code)
    }

    @Test
    fun `合法批通过校验`() {
        val decoded = decoder.decode(request(points = listOf(delta(0, 0, 0), delta(30_000, 5, 5))))

        assertNull(validator.validate(decoded, now, configFor()))
    }

    @Test
    fun `schema 主版本不符拒收`() {
        val decoded = decoder.decode(request(schemaVersion = "2.0"))

        assertEquals(ValidationCode.INVALID_SCHEMA_VERSION, validator.validate(decoded, now, configFor())?.code)
    }

    @Test
    fun `schema 次版本向前兼容`() {
        val decoded = decoder.decode(request(schemaVersion = "1.7"))

        assertNull(validator.validate(decoded, now, configFor()))
    }

    @Test
    fun `通道非法拒收`() {
        val decoded = decoder.decode(request(channel = Channel.CHANNEL_UNSPECIFIED))

        assertEquals(ValidationCode.PROTOCOL_ERROR, validator.validate(decoded, now, configFor())?.code)
    }

    @Test
    fun `实时通道超过 100 点拒收 补传通道 500 点内放行`() {
        val realtime = decoder.decode(request(points = List(101) { delta(0, 0, 0) }))
        assertEquals(ValidationCode.PAYLOAD_TOO_LARGE, validator.validate(realtime, now, configFor())?.code)

        val backfill = decoder.decode(request(points = List(101) { delta(0, 0, 0) }, channel = Channel.CHANNEL_BACKFILL))
        assertNull(validator.validate(backfill, now, configFor()))
    }

    @Test
    fun `d_ts 为负拒收并定位到该点`() {
        val decoded = decoder.decode(request(points = listOf(delta(0, 0, 0), delta(-1, 0, 0))))

        val failure = validator.validate(decoded, now, configFor())
        assertEquals(ValidationCode.TS_NOT_SORTED, failure?.code)
        assertEquals(1, failure?.idx)
    }

    @Test
    fun `坐标越界拒收`() {
        val decoded = decoder.decode(request(baseLatQ = 900_000_001))

        assertEquals(ValidationCode.INVALID_COORD, validator.validate(decoded, now, configFor())?.code)
    }

    @Test
    fun `实时通道超出 48h 窗口拒收 补传通道 7d 内放行`() {
        // 49h 前的点：实时通道拒绝，补传通道接受
        val old = now - 49L * 3_600_000
        val realtime = decoder.decode(request(baseTsMs = old, points = listOf(delta(0, 0, 0))))
        assertEquals(ValidationCode.TS_OUT_OF_WINDOW, validator.validate(realtime, now, configFor())?.code)

        val backfill = decoder.decode(request(baseTsMs = old, points = listOf(delta(0, 0, 0)), channel = Channel.CHANNEL_BACKFILL))
        assertNull(validator.validate(backfill, now, configFor()))
    }

    @Test
    fun `未来超过 10min 拒收`() {
        val future = now + 11L * 60_000
        val decoded = decoder.decode(request(baseTsMs = future, points = listOf(delta(0, 0, 0))))

        val failure = validator.validate(decoded, now, configFor())
        assertEquals(ValidationCode.TS_OUT_OF_WINDOW, failure?.code)
        assertNotNull(failure?.idx)
    }

    private fun configFor() =
        dev.yidafu.flowtrace.shared.protocol.validation
            .ValidationConfig()

    private fun request(
        points: List<TracePointDelta> = listOf(delta(0, 0, 0)),
        schemaVersion: String = "1.0",
        channel: Channel = Channel.CHANNEL_REALTIME,
        baseTsMs: Long = BASE_TS,
        baseLatQ: Int = 390_000_000,
    ): TraceBatchRequest =
        TraceBatchRequest
            .newBuilder()
            .setHeader(
                TraceBatchHeader
                    .newBuilder()
                    .setBatchId("0190f2a0-0000-7000-8000-0000000000aa")
                    .setSchemaVersion(schemaVersion)
                    .setDeviceId("dev-1")
                    .setUserId("u-1")
                    .setChannel(channel)
                    .setBase(
                        GeoBase
                            .newBuilder()
                            .setTsMs(baseTsMs)
                            .setLatQ(baseLatQ.toLong())
                            .setLngQ(1_160_000_000L),
                    ).setSdk(SdkInfo.newBuilder().setSdkVersion("1.0.0").setPlatform("android")),
            ).addAllPoints(points)
            .build()

    private fun delta(
        dTs: Long,
        dLat: Int,
        dLng: Int,
    ): TracePointDelta =
        TracePointDelta
            .newBuilder()
            .setDTs(dTs)
            .setDLat(dLat.toLong())
            .setDLng(dLng.toLong())
            .build()

    private companion object {
        /** 与测试固定时钟同一量级，保证默认批落在窗口内。 */
        const val BASE_TS = 1_795_000_000_000L
    }
}
