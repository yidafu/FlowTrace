package dev.yidafu.flowtrace.process.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

/**
 * 轨迹处理服务配置（`flowtrace.processor.*`，07 §3 / 08 §13）。
 *
 * 绑定方式与 upload 的 `UploadProperties` 一致：`@Component` + 可变属性 + kebab-case 键，
 * 由 flowtrace-app 的包扫描装配；键名与 `application.yaml` 的 `flowtrace.processor` 段一一对应。
 *
 * 单位约定写死在本类、不让调用方猜：`*Ms` = 毫秒、`*Sec` = 秒、`*Cm` = 厘米、`*M` = 米、
 * `*Mps` = 米/秒，其余为计数。默认值即 06 文档给出的默认口径——改默认值要同步 06/07/08，
 * 否则文档与运行值静默漂移。
 */
@Component
@ConfigurationProperties(prefix = "flowtrace.processor")
class ProcessorProperties {
    // ---------------------------------------------------------------- 消费

    /**
     * Kafka 引导地址，默认与 `spring.kafka.bootstrap-servers` / `application.yaml` 的
     * `spring.kafka.bootstrap-servers: ${KAFKA_BOOTSTRAP:localhost:9092}` 保持一致。
     *
     * 为什么不直接读 Spring 的 `spring.kafka.*`：本模块自建 consumer factory（`AckMode.MANUAL`
     * + 自定义错误处理器，Boot 的自动配置给不了），手工装配时把地址来源收敛到本类一处，
     * 拆独立部署时不用改代码、只覆盖 `flowtrace.processor.*` 即可。
     * 若想严格单一来源，可在 yaml 里写 `bootstrap-servers: ${spring.kafka.bootstrap-servers}` 绑定过来。
     */
    var bootstrapServers: String = "localhost:9092"

    /** 消费组：与 `realtime-group` 分开，避免多模块单体里两套消费逻辑撞组（06 §3.1）。 */
    var consumerGroup: String = "processor-group"

    /**
     * 监听并发数，等于 topic 分区数（本地 `trace.batch.v1` 为 8）；**并发大于分区数只是空转**
     * （多出来的消费者永远分不到分区）。改 topic 分区数时这里要同步改。
     */
    var concurrency: Int = 8

    /** 单次 poll 最大记录数：批处理的单位，权衡 DB 事务往返与失败重放放大。 */
    var pollRecords: Int = 100

    // ---------------------------------------------------------------- 水位与迟到

    /** 乱序水位窗口（10min）：批内最小 ts 早于「最大已见 ts − 该值」即判迟到批，跳过分段（06 §3.2）。 */
    var watermarkDriftMs: Long = 600_000

    // ---------------------------------------------------------------- 清洗（06 §3.3）

    /** 精度阈值（100m）：`accuracy` 超过它**且**距上一保留点不足 [cleanAccuracyLoneMs] 时丢点。 */
    var cleanAccuracyCm: Int = 10_000

    /** 精度规则的「孤立」时间窗（10min）：超过该时长即使精度差也保留，避免长途丢光点。 */
    var cleanAccuracyLoneMs: Long = 600_000

    /** 隐含速度上限（85 m/s ≈ 306 km/h）：超出的当前点视为定位漂移丢弃。 */
    var cleanSpeedMaxMps: Double = 85.0

    /** 「未来时间」容忍上限（10min）：端侧时钟漂移不该被整批丢弃，故不用 0。 */
    var futureMs: Long = 600_000

    // ---------------------------------------------------------------- 分段（06 §3.4）

    /** 段间时间断层阈值（10min）：超过即闭合当前段。 */
    var splitGapMs: Long = 600_000

    /** 段间位移跳变阈值（5000m）：超过即闭合当前段（定位漂移或跨城瞬移）。 */
    var splitJumpM: Double = 5_000.0

    /** 单段点数上限：达到即强制分段，防止长时间连续移动把内存与单行 WKT 拖爆（08 §8.5）。 */
    var openSegMaxPoints: Int = 5_000

    /** 设备状态 LRU 容量（每个 consumer）：淘汰时必须先 flush 落库再丢状态，否则段丢失。 */
    var deviceStateCacheSize: Int = 2_000

    // ---------------------------------------------------------------- 停留点（06 §3.5）

    /** 停留判定半径（50m）：簇内所有点与**锚点**（而非上一点或质心）的距离都不超过它。 */
    var stayRadiusM: Double = 50.0

    /** 停留最短时长（5min）：不足则不算停留，仍计入 MOVE 子段。 */
    var stayMinMs: Long = 300_000

    /** 相邻停留点合并间隔（2min）：更短的间隔视为同一次停留，合并后重算质心与时长。 */
    var stayMergeGapMs: Long = 120_000

    // ---------------------------------------------------------------- 存储写入

    /** 段几何抽稀 ε（5m，03 §3）：查询侧按缩放级别另用 LUT（06 §5.2），这里是落库口径。 */
    var dpStorageEpsilonM: Double = 5.0

    /** 点位批量写入的 JDBC batch 大小；需配合 URL 的 `reWriteBatchedInserts=true` 才有意义。 */
    var pointsWriteBatch: Int = 1_000

    // ---------------------------------------------------------------- 失败处理

    /** 单批处理墙钟上限：超过视为毒丸消息投 DLQ；DB 故障类不走这条（见 [retryBackoffMs]）。 */
    var batchTimeoutMs: Long = 60_000

    /** 可重试异常（DB/Redis 抖动）的固定退避：无限重试、不 commit，绝不进 DLQ（06 §3.7）。 */
    var retryBackoffMs: Long = 2_000

    /** 非可重试异常的投递上限，超过进 `trace.batch.dlq.v1`。 */
    var dlqMaxRetries: Int = 3

    // ---------------------------------------------------------------- 迟到重算（06 §3.6）

    /** 重算互斥锁 TTL（`recalc:u:{userId}`，5min）：按一个用户一天重算的最坏耗时估。 */
    var recalcLockTtlSec: Long = 300

    /** dirty_day 扫描间隔（60s）：越小重算越及时，越大越省一次空查询。 */
    var dirtyScanIntervalSec: Long = 60

    // ---------------------------------------------------------------- 段兜底收尾

    /** OPEN 段兜底扫描间隔（300s）：设备彻底沉默时靠它把尾段封口（06 §3.4）。 */
    var segmentFinalizeIntervalSec: Long = 300

    /** 单次封口上限：框住单事务锁行数，不是限制总吞吐。 */
    var segmentFinalizeBatch: Int = 500
}
