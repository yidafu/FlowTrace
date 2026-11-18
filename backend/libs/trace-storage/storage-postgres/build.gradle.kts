plugins {
    alias(libs.plugins.kotlin.jvm)
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
