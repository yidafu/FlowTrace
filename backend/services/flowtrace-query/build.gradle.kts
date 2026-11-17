plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
}

// 轨迹查询服务：回放、里程、简化轨迹
// 库模块：无独立启动类，由 services/flowtrace-app 统一装配
dependencies {
    api(project(":platform:common"))
}
