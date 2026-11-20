package dev.yidafu.flowtrace

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

/**
 * 单体应用唯一入口：扫描 dev.yidafu.flowtrace 全包。
 *
 * 包结构（与 Gradle 模块不强制一一对应，靠模块依赖约束分层）：
 * - `shared.model` / `shared.protocol`（domain 模块，不依赖 Spring）
 * - `shared.web` / `shared.cache` / `shared.persistence` / `shared.messaging`（infra 模块）
 * - `ingest` / `process` / `query` / `realtime` / `admin`（app 模块，业务包，禁止互相 import 实现类）
 */
@SpringBootApplication(scanBasePackages = ["dev.yidafu.flowtrace"])
@EnableScheduling
class FlowtraceApplication

fun main(args: Array<String>) {
    runApplication<FlowtraceApplication>(*args)
}
