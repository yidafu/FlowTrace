plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
}

// 轨迹接入服务：校验、幂等、限流，写 Kafka 后快速返回
// 库模块：无独立启动类，由 services/flowtrace-app 统一装配
dependencies {
    api(project(":platform:common"))
}
