plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
}

// Redis 存储库：platform/common 缓存抽象（FlowtraceCache）的 Redis 实现
// starter 用 implementation：Redis/Spring Data 类型不进入消费方编译类路径，
// services 只依赖 platform/common 的抽象接口，由 flowtrace-app 聚合本模块提供实现
dependencies {
    api(platform(libs.spring.boot.dependencies))

    api(project(":platform:common"))
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
}
