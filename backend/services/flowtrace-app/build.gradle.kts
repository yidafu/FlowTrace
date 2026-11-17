plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    alias(libs.plugins.spring.boot)
}

// 多模块单体唯一启动模块：聚合全部服务库模块
dependencies {
    implementation(platform(libs.spring.boot.dependencies))
    implementation("org.jetbrains.kotlin:kotlin-reflect")

    implementation(project(":platform:common"))
    implementation(project(":services:flowtrace-upload"))
    implementation(project(":services:flowtrace-processor"))
    implementation(project(":services:flowtrace-query"))
    implementation(project(":services:flowtrace-realtime"))
    implementation(project(":services:flowtrace-admin"))

    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("org.springframework.boot:spring-boot-starter-actuator-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
