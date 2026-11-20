plugins {
    alias(libs.plugins.kotlin.jvm)
    // all-open：@Component/@Configuration/@Repository 等 Spring 注解的类必须可被 CGLIB 代理。
    // Boot 4 的 spring-boot-persistence 会给每个 @Repository bean 挂
    // PersistenceExceptionTranslationAdvisor（spring.aop.proxy-target-class 默认 true），
    // 缺此插件时 Kotlin 类为 final，启动即报 "Could not generate CGLIB subclass"。
    alias(libs.plugins.kotlin.spring)
}

// 基础设施层：Web/可观测性通用件（原 platform/common）、Exposed + PostgreSQL 仓储、
// Redis 缓存实现、Kafka 生产端（原 trace-storage / trace-messaging）。
// 不引 spring-boot-starter-security：鉴权在前置网关（04 §1），后端不解析 JWT、不持会话，
// 只信任网关注入的 X-User-Id / X-Device-Id；引入后反而会生成内存用户并对全站挂 Basic 认证。
dependencies {
    api(platform(libs.spring.boot.dependencies))
    api(platform(libs.exposed.bom))

    // 仓储/协议类型出现在本模块公开签名，向上暴露给 app
    api(project(":domain"))

    // Web 通用件：TraceId/请求日志过滤器、GlobalExceptionHandler、ApiError、请求体脱敏
    api("org.springframework.boot:spring-boot-starter-webmvc")
    api("org.springframework.boot:spring-boot-starter-validation")
    api("org.springframework.boot:spring-boot-starter-actuator")
    api("io.micrometer:micrometer-registry-prometheus")
    // Boot 4 起 Jackson 3 坐标迁移到 tools.jackson
    api("tools.jackson.module:jackson-module-kotlin")

    // PostgreSQL + Exposed：表定义是 DDL 唯一事实源，migration 脚本由其与库结构差异生成
    api("org.springframework:spring-context")
    implementation("org.springframework.boot:spring-boot")
    api("org.jetbrains.exposed:exposed-core")
    api("org.jetbrains.exposed:exposed-jdbc")
    api("org.jetbrains.exposed:exposed-java-time")
    api("org.jetbrains.exposed:exposed-migration")
    // PostgreSQL 驱动版本由 Spring Boot BOM 管理
    api("org.postgresql:postgresql")

    // Kafka：TraceBatchProducer 公开签名含 spring-kafka 类型，用 api
    api("org.springframework.kafka:spring-kafka")
    // Boot 4 把 autoconfigure 按技术拆成独立模块，Kafka 自动配置在 spring-boot-kafka 而不在
    // spring-boot-autoconfigure 里。缺它就没人注册 KafkaListenerAnnotationBeanPostProcessor，
    // @KafkaListener 会静默失效（不报错、不告警，消费组永远不出现），生产者因手工装 factory 仍正常。
    api("org.springframework.boot:spring-boot-kafka")

    // Redis 实现：starter 用 implementation，Spring Data Redis 类型不进入消费方编译类路径，
    // app 只依赖本模块暴露的缓存抽象接口
    implementation("org.springframework.boot:spring-boot-starter-data-redis")

    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
