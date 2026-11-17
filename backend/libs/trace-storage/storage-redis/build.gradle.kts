plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    api(platform(libs.spring.boot.dependencies))

    api("org.springframework.boot:spring-boot-starter-data-redis")
}
