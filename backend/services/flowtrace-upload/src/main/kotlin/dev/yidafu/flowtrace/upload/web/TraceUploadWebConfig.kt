package dev.yidafu.flowtrace.upload.web

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.converter.protobuf.ProtobufJsonFormatHttpMessageConverter

/**
 * 上报 HTTP 编解码配置：注册 Protobuf 消息转换器（含 proto JSON 映射，依赖 protobuf-java-util），
 * 使 POST /v1/traces/batch 按 Content-Type 在 application/x-protobuf 与 application/json 间协商，
 * 控制器保持面向消息对象。Boot 会将该 HttpMessageConverter bean 合并进 MVC 转换器链。
 */
@Configuration
class TraceUploadWebConfig {
    @Bean
    fun protobufHttpMessageConverter(): ProtobufJsonFormatHttpMessageConverter = ProtobufJsonFormatHttpMessageConverter()
}
