package dev.yidafu.flowtrace.upload.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

/**
 * 上报服务配置（`flowtrace.upload.*`，07 §3）。
 *
 * 涵盖两部分：下发端侧的调速参数（`interval*`）与设备级限流阈值（`limit*`）。
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
