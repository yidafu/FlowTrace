plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    // 单体应用唯一启动模块：bootJar / bootRun 在此
    alias(libs.plugins.spring.boot)
}

// 应用层：轨迹接入（upload）、处理（processor）、查询（query）、实时位置（realtime）、
// 管理（admin）五个包 + 唯一启动类 FlowtraceApplication。
// 包边界即业务边界：包不跨层引用，日后若需拆分可按包整体移出。
dependencies {
    implementation(platform(libs.spring.boot.dependencies))
    implementation(project(":flowtrace-infra"))
    implementation(project(":flowtrace-domain"))

    implementation("org.jetbrains.kotlin:kotlin-reflect")
    // DataSourceAutoConfiguration + HikariCP，供 Exposed 接入 application.yaml 的 spring.datasource.*
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    // Protobuf 消息转换器的 application/json 支持（proto JSON 映射）
    implementation(libs.protobuf.java.util)
    // 设备状态/幂等等本地缓存；版本由 Spring Boot BOM 管理
    implementation("com.github.ben-manes.caffeine:caffeine")
    // 批记录异步落库的协程运行时（有界 Channel + 消费者协程）；版本由 Spring Boot BOM 管理
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")

    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("org.springframework.boot:spring-boot-starter-actuator-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    // 测试进程内为 Exposed transaction{} 提供事务管理器（PipelineFixture 注册 H2 内存库，
    // 只开事务、不执行 SQL）；版本由 Spring Boot BOM 管理
    testImplementation("com.h2database:h2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// 产物名固定为 flowtrace-app-<version>.jar：部署脚本、文档与镜像名沿用该名，
// 与模块目录名（flowtrace-app）无关，改名不影响产物
tasks.withType<org.springframework.boot.gradle.tasks.bundling.BootJar>().configureEach {
    archiveBaseName.set("flowtrace-app")
}
