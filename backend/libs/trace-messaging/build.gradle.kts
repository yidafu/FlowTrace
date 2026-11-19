plugins {
    alias(libs.plugins.kotlin.jvm)
    // all-open：@Component Producer / @Configuration 类需可被 CGLIB 代理
    alias(libs.plugins.kotlin.spring)
}

dependencies {
    api(platform(libs.spring.boot.dependencies))
    api(project(":platform:common"))
    // TraceBatchMessage 生成类来自协议模块，出现在 Producer 公开签名，用 api
    api(project(":libs:trace-protocol"))

    api("org.springframework.kafka:spring-kafka")
    // Boot 4 把 autoconfigure 按技术拆成独立模块，Kafka 自动配置在 spring-boot-kafka 而不在
    // spring-boot-autoconfigure 里。缺它就没人注册 KafkaListenerAnnotationBeanPostProcessor，
    // @KafkaListener 会静默失效（不报错、不告警，消费组永远不出现），生产者因手工装 factory 仍正常。
    api("org.springframework.boot:spring-boot-kafka")
}
