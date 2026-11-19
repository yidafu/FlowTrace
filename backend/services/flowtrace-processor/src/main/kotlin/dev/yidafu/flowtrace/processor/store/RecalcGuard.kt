package dev.yidafu.flowtrace.processor.store

import dev.yidafu.flowtrace.common.cache.FlowtraceCache
import org.springframework.stereotype.Component

@Component
class RecalcGuard(
    private val cache: FlowtraceCache,
) {
    fun isLocked(userId: String): Boolean = cache.has(keyOf(userId))

    fun tryLock(userId: String): Boolean {
        if (isLocked(userId)) return false
        cache.put(keyOf(userId), userId)
        return true
    }

    fun unlock(userId: String) {
        cache.evict(keyOf(userId))
    }

    fun keyOf(userId: String) = "recalc:u:$userId"
}
