package dev.yidafu.flowtrace.ingest.web

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
    /**
     * 同时支持两种格式的转换器：`application/x-protobuf` 按二进制解析、`application/json`
     * 按 proto 的 JSON 映射解析（两者产出的都是同一个 `TraceBatchRequest`）。
     *
     * 用 ProtobufJsonFormat 版而不是纯二进制版，是为了让 protoc/curl 调试与兼容场景能直接发 JSON；
     * 生产端侧仍只发 Protobuf（04 §2），JSON 不承担主链路带宽。
     */
    @Bean
    fun protobufHttpMessageConverter(): ProtobufJsonFormatHttpMessageConverter = ProtobufJsonFormatHttpMessageConverter()
}
