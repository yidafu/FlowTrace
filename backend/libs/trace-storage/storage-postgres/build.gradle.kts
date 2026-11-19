plugins {
    alias(libs.plugins.kotlin.jvm)
    // all-open：@Repository/@Component 等 Spring 注解的类必须可被 CGLIB 继承。
    // Boot 4 的 spring-boot-persistence 会给每个 @Repository bean 挂
    // PersistenceExceptionTranslationAdvisor（spring.aop.proxy-target-class 默认 true），
    // 缺此插件时 Kotlin 类为 final，启动即报 "Could not generate CGLIB subclass"。
    alias(libs.plugins.kotlin.spring)
}

dependencies {
    api(platform(libs.spring.boot.dependencies))
    api(platform(libs.exposed.bom))
    api(project(":libs:trace-model"))
    // @Repository / @Configuration / CommandLineRunner 由 app 组件扫描装配
    api("org.springframework:spring-context")
    implementation("org.springframework.boot:spring-boot")

    api("org.jetbrains.exposed:exposed-core")
    api("org.jetbrains.exposed:exposed-jdbc")
    api("org.jetbrains.exposed:exposed-java-time")
    // DDL 变更管理：migration 脚本由表定义与库结构差异生成
    api("org.jetbrains.exposed:exposed-migration")
    // PostgreSQL 驱动版本由 Spring Boot BOM 管理
    api("org.postgresql:postgresql")
}
