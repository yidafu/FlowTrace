plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
}

// 管理服务：设备、保留策略、审计
// 库模块：无独立启动类，由 services/flowtrace-app 统一装配
dependencies {
    api(project(":platform:common"))
}
