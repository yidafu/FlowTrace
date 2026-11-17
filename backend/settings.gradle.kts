// FlowTrace 后端多模块工程（多模块单体：flowtrace-app 为唯一启动模块）
// 模块划分见仓库根目录 AGENTS.md「目录结构」一节。
pluginManagement {
    repositories {
        // 国内加速：腾讯云 Maven 镜像优先，Plugin Portal 兜底
        maven("https://mirrors.cloud.tencent.com/nexus/repository/maven-public/")
        gradlePluginPortal()
    }
}

rootProject.name = "flowtrace-backend"

include(
    // 平台层：通用能力模块（原 common-core/web/security/observability 合并）
    ":platform:common",
    // 领域库
    ":libs:trace-model",
    ":libs:trace-protocol",
    ":libs:trace-messaging",
    ":libs:trace-storage:storage-postgres",
    ":libs:trace-storage:storage-redis",
    // 服务（库模块，业务边界保留）
    ":services:flowtrace-upload",
    ":services:flowtrace-processor",
    ":services:flowtrace-query",
    ":services:flowtrace-realtime",
    ":services:flowtrace-admin",
    // 唯一启动模块：聚合全部服务
    ":services:flowtrace-app",
)
