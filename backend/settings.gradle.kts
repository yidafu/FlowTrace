// FlowTrace 后端：单体应用，三层模块（domain <- infra <- app）
// 模块职责见仓库根目录 AGENTS.md「目录结构」一节。
pluginManagement {
    repositories {
        // 国内加速：腾讯云 Maven 镜像优先，Plugin Portal 兜底
        maven("https://mirrors.cloud.tencent.com/nexus/repository/maven-public/")
        gradlePluginPortal()
    }
}

rootProject.name = "flowtrace-backend"

include(
    // 领域层：轨迹点/段/批次等纯数据模型 + 上报协议（Delta 编解码、批校验）。不得依赖 Spring
    ":domain",
    // 基础设施层：Web/可观测性通用件 + Exposed/PostgreSQL + Redis + Kafka
    ":infra",
    // 唯一启动模块：全部 HTTP 接口与处理流水线
    ":app",
)
