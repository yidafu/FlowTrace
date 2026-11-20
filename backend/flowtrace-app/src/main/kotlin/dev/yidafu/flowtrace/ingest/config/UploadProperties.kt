package dev.yidafu.flowtrace.ingest.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

/**
 * 上报服务配置（`flowtrace.upload.*`，07 §3）。
 *
 * 三部分：下发端侧的调速参数（`interval*`）、设备级限流阈值（`limit*`）、
 * 字段校验阈值（`max-points-*` / `ts-window-*`，06 §2.3）。
 * 限流窗口固定 60s，键名 `*-per-min` 已编码该语义，故不单独配置化。
 */
@Component
@ConfigurationProperties(prefix = "flowtrace.upload")
class UploadProperties {
    /** 实时通道批数上限（批/分钟），04 §3 限流。 */
    var limitBatchPerMinRt: Int = 12

    /** 补传通道批数上限（批/分钟）；补传是大批低频，配额比实时通道更紧。 */
    var limitBatchPerMinBf: Int = 6

    /** 单设备点数上限（点/分钟），两通道共享额度。 */
    var limitPointsPerMin: Int = 6000

    /** 实时通道单批点数上限（06 §2.3：≤100）。 */
    var maxPointsRt: Int = 100

    /** 补传通道单批点数上限（06 §2.3：≤500）。 */
    var maxPointsBf: Int = 500

    /** 实时通道点 ts 允许的最早偏移（48h，06 §2.3）。 */
    var tsWindowRtPastMs: Long = 172_800_000L

    /** 补传通道点 ts 允许的最早偏移（7d，06 §2.3）。 */
    var tsWindowBfPastMs: Long = 604_800_000L

    /** 点 ts 的未来容忍（10min，06 §2.3）：端侧时钟漂移不整批拒收。 */
    var tsWindowFutureMs: Long = 600_000L

    /** 基础上报间隔（秒），04 §2.3 的 base。 */
    var intervalBaseSec: Int = 30

    /** 被限流（429）时下发的间隔（秒），同时作为 Retry-After。 */
    var intervalLimitedSec: Int = 300

    /** 下发值下限（秒），与端侧钳制区间一致。 */
    var intervalMinSec: Int = 10

    /** 下发值上限（秒），与端侧钳制区间一致。 */
    var intervalMaxSec: Int = 300

    /** 钳制到 [intervalMinSec]..[intervalMaxSec]。 */
    fun clamp(seconds: Int): Int = seconds.coerceIn(intervalMinSec, intervalMaxSec)
}
