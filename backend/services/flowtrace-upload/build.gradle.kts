plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
}

// 轨迹接入服务：校验、幂等、限流，写 Kafka 后快速返回
// 库模块：无独立启动类，由 services/flowtrace-app 统一装配
dependencies {
    api(project(":platform:common"))
    // 上报协议事实源（proto/）的生成代码：消息类型出现在控制器公开签名，用 api
    api(project(":libs:trace-protocol"))
    implementation(project(":libs:trace-storage:storage-redis"))
    implementation(project(":libs:trace-storage:storage-postgres"))
    implementation(project(":libs:trace-messaging"))
    // Protobuf 消息转换器的 application/json 支持（proto JSON 映射）
    implementation(libs.protobuf.java.util)
    // 设备状态/幂等等本地缓存；版本由 Spring Boot BOM 管理
    implementation("com.github.ben-manes.caffeine:caffeine")

    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
