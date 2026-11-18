plugins {
    alias(libs.plugins.kotlin.jvm)
}

// 多模块单体下的通用平台模块：Web / 可观测性统一在此聚合
// （原 common-core/web/security/observability 四模块合并）
// 不引 spring-boot-starter-security：鉴权在前置网关（04 §1），后端不解析 JWT、不持会话，
// 只信任网关注入的 X-User-Id / X-Device-Id；引入后反而会生成内存用户并对全站挂 Basic 认证。
dependencies {
    api(platform(libs.spring.boot.dependencies))

    api("org.springframework.boot:spring-boot-starter-webmvc")
    api("org.springframework.boot:spring-boot-starter-validation")
    api("org.springframework.boot:spring-boot-starter-actuator")
    api("io.micrometer:micrometer-registry-prometheus")
    // Boot 4 起 Jackson 3 坐标迁移到 tools.jackson
    api("tools.jackson.module:jackson-module-kotlin")

    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
