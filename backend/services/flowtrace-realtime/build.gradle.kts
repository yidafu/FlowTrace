plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
}

// 实时位置服务：最新位置（Redis）
// 库模块：无独立启动类，由 services/flowtrace-app 统一装配
dependencies {
    api(project(":platform:common"))
}
