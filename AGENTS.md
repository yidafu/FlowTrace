# FlowTrace

轨迹采集与回溯平台：App 端采集定位点 → 本地队列缓存 → 批量压缩上报 → Kafka → 异步处理 → 存储 → 查询/回放。为巡检等业务提供轨迹佐证，但**不承担巡检业务逻辑**。

工程骨架已于 2026-11-17 初始化：`backend/` 为 Spring Boot 4.1.2 多模块 Gradle 工程（由 Spring Initializr 官方种子重构），`sdk/react-native-flowtrace/` 由 create-react-native-library 0.64 生成（turbo-module，Kotlin + ObjC）。构建/测试/Lint 命令见下文「构建命令」。

## 硬性约束（优先级最高）

- **除非用户明确指示，不要动手写代码**：不创建代码文件、不生成脚手架、不初始化工程。默认工作方式是讨论、方案设计、权衡评估和更新本文档/文档类文件；仅当用户明确说「写代码 / 实现 / 生成」后才进入编码。

## 目录结构（已初始化）

```
proto/                              # 轨迹上报协议（Protobuf + Delta 编码）
backend/
├── platform/                       # common（原 core/web/security/observability 四合一）
├── libs/                           # trace-model / trace-protocol / trace-messaging
│   └── trace-storage/              # storage-postgres / storage-redis
└── services/                       # 服务为库模块（无独立启动类）
    ├── flowtrace-upload/           # 轨迹接入：校验、幂等、限流、写 Kafka
    ├── flowtrace-processor/        # 轨迹处理：清洗、去重、抽稀、分段、停留点
    ├── flowtrace-query/            # 轨迹查询：回放、里程、简化轨迹
    ├── flowtrace-realtime/         # 最新位置（Redis）
    ├── flowtrace-admin/            # 设备/保留策略/审计
    └── flowtrace-app/              # 唯一启动模块：聚合全部服务（多模块单体）
sdk/react-native-flowtrace/         # 标准 RN 库工程（turbo-module：Kotlin + ObjC，bob 构建）
├── src/                            # TS 源码，对外 API 由 src/index.ts 导出
│   ├── core/                       # 采样、批量、重试
│   ├── storage/                    # 本地队列抽象（SQLite）
│   ├── transport/                  # 上传、Protobuf + Delta 编码（生成代码在 transport/generated/）
│   ├── location/                   # 定位抽象
│   └── ui/                         # 轨迹管理页面（可选子入口 ./ui，MapLibre；peer 依赖不进主入口）
├── android/                        # 原生 Android 模块（Kotlin，含 Room）
├── ios/                            # 原生 iOS 模块（Objective-C）
├── lib/                            # bob 构建产物（gitignore，不手改）
├── package.json / tsconfig.json / Flowtrace.podspec
examples/                           # 示例 App（含 RN 示例工程）
deploy/
```

## 构建命令

### 后端（backend/，Gradle Kotlin DSL + 版本目录 libs.versions.toml）

- 构建：`cd backend && ./gradlew build`
- 测试：`./gradlew test`
- Lint / 格式化：`./gradlew ktlintCheck` / `./gradlew ktlintFormat`
- 本地运行（唯一启动模块）：`./gradlew :services:flowtrace-app:bootRun`
- Protobuf 生成：随 `./gradlew build` 自动执行（`libs/trace-protocol`，协议事实源在仓库根目录 `proto/`；protoc/运行时版本统一在 `gradle/libs.versions.toml`）

技术栈：Spring Boot 4.1.2 / Kotlin 2.3.21 / Java 21 工具链 / Gradle 9.9.1（wrapper）。Boot 4 注意：starter 命名为 `spring-boot-starter-webmvc`，Jackson 3 坐标在 `tools.jackson`。Gradle 发行版与依赖仓库均走腾讯云镜像（wrapper `distributionUrl` + `pluginManagement`/`repositories`），换回官方源需同步改这三处。

### RN SDK（sdk/react-native-flowtrace/，yarn 4 workspace + bob）

- 安装依赖：`yarn install`
- 构建（bob → lib/）：`yarn prepare`
- 类型检查：`yarn typecheck`
- Lint：`yarn lint`
- 测试：`yarn test`
- Protobuf TS 生成：`yarn proto:generate`（buf + protobuf-es → `src/transport/generated/`，生成代码随源码入库）
- 示例 App：`yarn example ios` / `yarn example android`

## 架构边界（编辑代码前必读）

- 主链路：`采集 -> 本地队列 -> 批量上报 -> 接入 -> Kafka -> 处理 -> 存储 -> 查询`。主链路只做轨迹的采集、上报、处理、查询。
- 巡检业务通过 `taskId/sessionId` 关联和消费轨迹结果，**不得侵入上报主链路**。
- `flowtrace-upload` 只做校验、幂等、写 Kafka 后快速返回；重处理逻辑放 `flowtrace-processor`。
- **当前为多模块单体（modular monolith）**：`services/flowtrace-app` 是唯一启动模块，upload/processor/query/realtime/admin 均为库模块；各服务保持独立包/模块边界，未来需要时（尤其 processor/realtime）可拆出独立启动模块部署。
- 存储分工（P 阶段）：轨迹点存 PostgreSQL **普通表**，查询走 `(user_id, ts)` 索引；轨迹段 geometry 以 WKT/GeoJSON **文本列**存储；最新位置 → Redis；冷数据 → 对象存储 Parquet。原始点与轨迹段分表。
- 空间计算全部在应用层：flowtrace-processor / flowtrace-query 用 **JTS** 做抽稀、里程、分段、点到巡检点距离计算，不在数据库做地理查询。
- **明确决策：不引入 ClickHouse，P 阶段不启用 PostGIS 扩展**（复杂度/运维负担过高）。高容量靠动态采样、按天分区、冷数据归档解决，而不是加存储引擎或扩展；后续不要在方案中再提议这两者。

## 关键设计约定

- **端侧采样**：动态采样（静止 5min 降频、步行 20s/20m、骑行 10s/10m、驾车 5-10s/30m、低电量降频）；SQLite 本地队列上限 7 天/10 万点 FIFO 淘汰。采集与上报解耦：采集器只写本地队列，上报器独立调度。
- **端侧上报（双通道，保证实时性）**：
  - **实时通道**：满足任一条件即触发小批上报——距上次成功上报 ≥30s 且有新点 / 攒够 ~20 点 / 运动状态切换 / 进入巡检点半径（关键点优先）。小批（≤50 点）、任意可用网络即发（弱网也保发），节奏由服务端 `nextUploadIntervalSec` 动态调节。
  - **补传通道**：历史积压点在 WiFi/充电时大批上传（≤500 点或 ≤256KB），不与实时通道竞争。
  - 重试指数退避加抖动（5s/30s/2min/10min/30min）；实时通道失败不阻塞补传通道。
- **编码链路（统一约定）**：上报只用 Protobuf + Delta + GZIP，不用 JSON。批头放基准值（base ts/lat/lng），批内点字段做增量编码，坐标整数量化（如 ×1e7），Protobuf 序列化后整体 GZIP。SDK `transport/` 与服务端解码保持同一约定。
- **幂等**：`batchId` 全局唯一（唯一索引防重），点级 `pointId` 去重。任何写入路径都要保持这一语义。
- **顺序**：Kafka 按 `userId/deviceId` 分区保证同设备有序；允许 10 分钟乱序窗口，超迟数据走补录通道；消费端幂等写。
- **API**：上报 `POST /v1/traces/batch`（Bearer token，gzip，响应含 `nextUploadIntervalSec` 供端侧动态调速）；查询默认返回简化轨迹（`simplify=true`），回放按缩放级别取点。
- **限流**：按设备限批次数/点数，异常设备降级或拒绝。
- **隐私**：撤回授权后停止采集并清理未上报数据；删除用户轨迹 `DELETE /v1/users/{userId}/traces` 必须覆盖所有存储（含 Kafka 之后的全部落盘）。
- **目标指标**：上报成功率 >99%，实时通道最新点端到端 ≤30s（网络正常时），整体端到端 <1 分钟，查询 P95 <500ms。容量按 10 亿点/天设计，任何设计不得假设全量高频存储。

## 数据模型

三张核心表：`trace_point`（原始点，`(user_id, ts)` 索引）、`trace_segment`（轨迹段，geometry 存 WKT/GeoJSON 文本列）、`trace_batch`（上报批次）。字段以项目 spec 为准；改动字段需同步 proto/、Exposed 表定义与 SDK 序列化。

## 约定

- 本地依赖：Kafka（KRaft 单节点，`localhost:9092`）与 Redis 7（`localhost:6379`）用 `podman compose up -d` 拉起，见 `deploy/`；PostgreSQL 另行本地安装。
- 后端技术栈：Kotlin + Spring Boot + Exposed ORM（普通 PostgreSQL，无 PostGIS 扩展；版本集中在 `backend/gradle/libs.versions.toml`）。**DDL 变更用 Exposed migration（exposed-migration 模块，1.0.0-beta 线）管理**：表定义（Exposed Table 对象）仍是唯一事实源，migration 脚本由表定义与库结构差异生成后随代码入库；不引入 Flyway/Liquibase。
- 几何一律「文本序列化（WKT/GeoJSON 字符串列）+ 应用层 JTS 计算」；不要引入 PostGIS，也不要为 Exposed 写自定义几何列类型。
- SDK 采用**标准 React Native 库工程**（create-react-native-library / react-native-builder-bob + codegen 形态）：TS 源码在 `src/`，原生模块在顶层 `android/`、`ios/`，构建产物 `lib/` 不入库；不使用 `src/native/` 这类非标准目录。原生侧为 turbo-module 官方组合：Android 用 Kotlin（codegen 包名 `dev.yidafu.flowtrace`），iOS 用 Objective-C（Swift 需走 nitro 三方路径，已评估并放弃）。
- 代码注释与文档使用中文（与现有 spec 一致）。
- 涉及协议（proto/）或数据模型改动的 PR，需同时考虑服务端、SDK、存储三端的兼容性。
