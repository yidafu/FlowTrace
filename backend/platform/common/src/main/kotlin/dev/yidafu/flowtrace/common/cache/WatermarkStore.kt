package dev.yidafu.flowtrace.common.cache

interface WatermarkStore {
    fun maxSeenTs(deviceId: String): Long?

    fun writeIfGreater(
        deviceId: String,
        tsMs: Long,
    )
}
