plugins {
    alias(libs.plugins.kotlin.jvm)
}

// 多模块单体下的通用平台模块：Web / Security / 可观测性统一在此聚合
// （原 common-core/web/security/observability 四模块合并）
dependencies {
    api(platform(libs.spring.boot.dependencies))

    api("org.springframework.boot:spring-boot-starter-webmvc")
    api("org.springframework.boot:spring-boot-starter-validation")
    api("org.springframework.boot:spring-boot-starter-security")
    api("org.springframework.boot:spring-boot-starter-actuator")
    api("io.micrometer:micrometer-registry-prometheus")
    // Boot 4 起 Jackson 3 坐标迁移到 tools.jackson
    api("tools.jackson.module:jackson-module-kotlin")
}
