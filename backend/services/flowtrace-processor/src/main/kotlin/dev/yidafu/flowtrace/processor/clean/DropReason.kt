package dev.yidafu.flowtrace.processor.clean

/**
 * 清洗丢弃原因（06 §3.3 指标 tag）。顺序与规则表一致，便于指标聚合时对接文档。
 */
enum class DropReason {
    /** 零岛：lat_q == 0 && lng_q == 0。 */
    ZERO,

    /** 未来时间：ts > now + futureMs。 */
    FUTURE,

    /** 与上一保留点的 (ts, lat_q, lng_q) 完全相同。 */
    DUPLICATE,

    /** 精度过差且距上一保留点不足 cleanAccuracyLoneMs。 */
    ACCURACY,

    /** 与上一保留点隐含速度超阈值，或同一毫秒发生位移。 */
    SPEED,
}
