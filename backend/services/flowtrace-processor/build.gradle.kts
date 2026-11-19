plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
}

// 轨迹处理服务：清洗、去重、抽稀、分段、停留点
// 库模块：无独立启动类，由 services/flowtrace-app 统一装配
dependencies {
    api(project(":platform:common"))
    api(project(":libs:trace-messaging"))
    implementation(project(":libs:trace-storage:storage-postgres"))
    implementation(project(":libs:trace-storage:storage-redis"))

    testImplementation(kotlin("test"))
}
