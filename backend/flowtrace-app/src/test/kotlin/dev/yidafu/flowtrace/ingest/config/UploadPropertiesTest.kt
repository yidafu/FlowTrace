package dev.yidafu.flowtrace.ingest.config

import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Configuration
import kotlin.test.assertEquals

/**
 * 配置绑定回归：断言 `application.yaml` / 07 §3 里约定的 kebab-case 键能落到属性上。
 *
 * 限流阈值原本硬编码在 ReteLimiter 内，迁移后最容易出的错是「键名改了、绑定没跟上」——
 * 那种情况下 yaml 静默失效、限流退回默认值，线上表现为配置改了没反应，故单独锁一层。
 */
class UploadPropertiesTest {
    private val runner =
        ApplicationContextRunner()
            .withUserConfiguration(TestConfig::class.java)
            .withPropertyValues(
                "flowtrace.upload.limit-batch-per-min-rt=15",
                "flowtrace.upload.limit-batch-per-min-bf=7",
                "flowtrace.upload.limit-points-per-min=8000",
            )

    @Test
    fun `限流阈值按约定键名绑定`() {
        runner.run { ctx ->
            val props = ctx.getBean(UploadProperties::class.java)
            assertEquals(15, props.limitBatchPerMinRt)
            assertEquals(7, props.limitBatchPerMinBf)
            assertEquals(8000, props.limitPointsPerMin)
        }
    }

    @Test
    fun `缺省值与 07 文档一致`() {
        ApplicationContextRunner()
            .withUserConfiguration(TestConfig::class.java)
            .run { ctx ->
                val props = ctx.getBean(UploadProperties::class.java)
                assertEquals(12, props.limitBatchPerMinRt)
                assertEquals(6, props.limitBatchPerMinBf)
                assertEquals(6000, props.limitPointsPerMin)
            }
    }

    @Configuration
    @EnableConfigurationProperties(UploadProperties::class)
    class TestConfig
}
