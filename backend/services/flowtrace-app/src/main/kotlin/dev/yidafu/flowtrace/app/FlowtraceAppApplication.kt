package dev.yidafu.flowtrace.app

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

/**
 * 多模块单体唯一入口：扫描 dev.yidafu.flowtrace 全包。
 * 各服务模块保持独立包/模块边界，未来需要时（尤其 processor/realtime）可拆出独立启动模块部署。
 */
@SpringBootApplication(scanBasePackages = ["dev.yidafu.flowtrace"])
@EnableScheduling
class FlowtraceAppApplication

fun main(args: Array<String>) {
    runApplication<FlowtraceAppApplication>(*args)
}
