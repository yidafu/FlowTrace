package dev.yidafu.flowtrace.common.cache

import java.time.Duration

interface TombstoneStore {
    fun has(userId: String): Boolean

    fun mark(
        userId: String,
        jobId: String,
        ttl: Duration,
    )
}
