package dev.yidafu.flowtrace.shared.cache

interface WatermarkStore {
    fun maxSeenTs(deviceId: String): Long?

    fun writeIfGreater(
        deviceId: String,
        tsMs: Long,
    )
}
