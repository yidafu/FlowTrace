plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    api(platform(libs.spring.boot.dependencies))
    api(project(":platform:common"))

    api("org.springframework.kafka:spring-kafka")
}
