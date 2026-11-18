plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    api(platform(libs.spring.boot.dependencies))
    api(project(":platform:common"))
    // TraceBatchMessage 生成类来自协议模块，出现在 Producer 公开签名，用 api
    api(project(":libs:trace-protocol"))

    api("org.springframework.kafka:spring-kafka")
}
