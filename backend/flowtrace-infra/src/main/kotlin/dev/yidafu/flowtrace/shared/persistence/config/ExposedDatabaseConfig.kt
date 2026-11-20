package dev.yidafu.flowtrace.shared.persistence.config

import org.jetbrains.exposed.v1.jdbc.Database
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.sql.DataSource

/**
 * 把 Boot 自动配置的 Hikari DataSource 接入 Exposed（transaction{} 才能拿到连接）。
 * 连接惰性建立：本地无 PG 时应用仍可启动，首次查询才报错。
 */
@Configuration(proxyBeanMethods = false)
class ExposedDatabaseConfig {
    @Bean
    fun exposedDatabase(dataSource: DataSource): Database = Database.connect(dataSource)
}
