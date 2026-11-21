package dev.yidafu.flowtrace.shared.cache

interface WatermarkStore {
    fun maxSeenTs(deviceId: String): Long?

    /**
     * 批量读水位（处理合批链路用，08 §4）：一次 MGET 覆盖全部设备。
     *
     * 只返回**有水位**的设备（含存量脏值按「无水位」跳过的口径，与 [maxSeenTs] 一致）；
     * 缺席即「该设备还没有历史点」，调用方按不判迟到处理。空入参直接返回空 Map，不打 Redis。
     */
    fun maxSeenTsBatch(deviceIds: Collection<String>): Map<String, Long>

    fun writeIfGreater(
        deviceId: String,
        tsMs: Long,
    )

    /**
     * 批量比较写（08 §4）：一次多键 EVAL 覆盖 `device → 批尾 ts`。
     *
     * 逐键语义与 [writeIfGreater] 完全一致（GET → 比较 → SET 原子执行）；同设备多批的场景
     * 由调用方先取最大再传入，不在本方法内去重。空 Map 直接返回。
     */
    fun writeIfGreaterBatch(entries: Map<String, Long>)
}
