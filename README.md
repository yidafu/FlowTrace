# FlowTrace

轨迹采集与回溯平台。从 App 端可靠采集定位点，弱网/离线不丢数据，批量压缩上报；服务端高吞吐接收、异步清洗处理，支持轨迹查询、回放、里程统计、停留点/轨迹段分析，可为巡检等业务提供轨迹佐证。

> **状态**：工程骨架已初始化（Spring Boot 多模块后端 + RN SDK 库工程 + proto 协议），业务实现按设计文档推进中。技术决策与工程约定见 [AGENTS.md](AGENTS.md)；详细设计（协议/存储/API/SDK/服务端/部署，可参考实现级别）见 [docs/design/](docs/design/README.md)。

## 核心链路

```
App端（RN SDK）
  定位采集 -> 动态采样 -> 本地队列(SQLite)
        -> 双通道上报(实时小批 / WiFi充电补传大批)
        -> Protobuf + Delta 编码 + GZIP
                    |
                    v
API网关（TLS、鉴权、按设备限流）
                    |
                    v
flowtrace-upload：校验、幂等、写 Kafka 后快速返回
                    |
                    v
Kafka: trace.batch.v1（按 deviceId 分区）
                    |
        +-----------+-----------+
        |                       |
flowtrace-processor        flowtrace-realtime
清洗/去重/抽稀/分段/停留点   更新 Redis 最新位置
（JTS 应用层计算）           |
        |                       |
        +-----------+-----------+
                    |
        PostgreSQL（普通表） / Redis / 对象存储（冷数据）
                    |
              flowtrace-query
        轨迹查询/回放/里程/简化轨迹
```

巡检等业务服务只消费轨迹结果或按 `taskId/sessionId` 查询，不阻塞、不侵入上报主链路。

## 技术栈与关键决策

| 领域 | 决策 |
|---|---|
| 端侧 SDK | React Native；动态采样 + SQLite 本地队列 + 双通道上报 |
| 上报编码 | Protobuf + Delta 编码（批头基准值、坐标整数量化）+ GZIP，不用 JSON |
| 后端语言 | Kotlin |
| ORM | Exposed，表结构由代码管理（无 SQL migration 文件、无 Flyway/Liquibase） |
| 主存储 | PostgreSQL **普通表**：轨迹点按 `(user_id, ts)` 索引；轨迹段 geometry 存 WKT/GeoJSON 文本列 |
| 空间计算 | 应用层 JTS（抽稀、里程、分段、点到巡检点距离），**不使用 PostGIS 扩展** |
| 最新位置 | Redis |
| 消息队列 | Kafka，按 `userId/deviceId` 分区保证同设备有序 |
| 冷数据 | 对象存储 Parquet，保留 6~12 个月 |
| 明确不做 | 不引入 ClickHouse；P 阶段不启用 PostGIS 扩展（复杂度/运维负担过高） |

## 端侧策略

**动态采样**（按运动状态自适应，避免固定高频）：

| 状态 | 策略 |
|---|---|
| 静止 | 5 分钟后降频或暂停 |
| 步行 | 20~30 秒或 20 米 |
| 骑行 | 10 秒或 10 米 |
| 驾车 | 5~10 秒或 30 米 |
| 低电量 | 降频，优先保证关键点 |
| 巡检任务期间 | 进入任务/巡检点半径前后提高采样频率 |

**双通道上报（保证实时性）**：
- 实时通道：距上次成功上报 ≥30s 有新点 / 攒够 ~20 点 / 运动状态切换 / 进入巡检点半径，任一触发小批上报（≤50 点），任意可用网络即发；节奏受服务端 `nextUploadIntervalSec` 动态调节。
- 补传通道：历史积压在 WiFi/充电时大批上传（≤500 点或 ≤256KB）。
- 失败重试：指数退避 + 随机抖动（5s / 30s / 2min / 10min / 30min）。
- 本地队列上限 7 天 / 10 万点，FIFO 淘汰；撤回定位授权后停止采集并清理未上报数据。

## 数据模型

| 表 | 说明 |
|---|---|
| `trace_point` | 原始轨迹点，`(user_id, ts)` 索引 |
| `trace_segment` | 轨迹段（起止、里程、时长、点数），geometry 存 WKT/GeoJSON 文本列 |
| `trace_batch` | 上报批次，`batchId` 唯一防重 |

幂等语义：`batchId` 批级唯一索引，`pointId` 点级去重；允许 10 分钟乱序窗口，超迟数据走补录通道。

## API 概览

```http
POST   /v1/traces/batch              # 批量上报（Protobuf + Delta + GZIP）
GET    /v1/traces                    # 查询轨迹（默认简化，可按缩放级别取点）
GET    /v1/trace-segments            # 查询轨迹段
DELETE /v1/users/{userId}/traces     # 删除用户轨迹（覆盖全部落盘存储）
```

上报响应携带 `nextUploadIntervalSec`，端侧据此动态调整实时通道节奏。

## 目标指标

- 有网上报成功率 >99%
- 实时通道最新点端到端 ≤30s（网络正常时），整体端到端 <1 分钟
- 查询 P95 <500ms
- 容量按 10 亿点/天设计：靠动态采样、按天分区、冷数据归档应对，而非引入更多存储引擎

## 规划目录

```
proto/                          # 轨迹上报协议（Protobuf + Delta 编码）
backend/                        # 单体应用：三层模块，flowtrace-domain <- flowtrace-infra <- flowtrace-app
├── flowtrace-domain/           # 领域层：shared/model（模型/几何）+ shared/protocol（Delta 解码、批校验），不依赖 Spring
├── flowtrace-infra/            # 基础设施层：shared/web、shared/cache(+impl)、shared/persistence、shared/messaging
└── flowtrace-app/              # 唯一启动模块：ingest / process（JTS）/ query / realtime / admin + 启动类
sdk/react-native-flowtrace/     # 标准 RN 库工程：src(core/storage/transport/location) + android/ + ios/
examples/
deploy/
```

## 演进路线

1. **MVP**：RN SDK（动态采样 + SQLite 队列 + 双通道上报）+ 网关 + flowtrace-upload + Kafka + PostgreSQL 普通表 + Redis + 异步处理 + 查询 + 基础管理后台。
2. **演进**：轨迹管理页面（本地队列/上报状态可视化 + 手动补报 + MapLibre 地图，见详细设计 05 §9）；实时位置、地理围栏、停留点识别；冷数据归档与自动降采样；巡检扩展（`taskId/sessionId` 关联轨迹、打卡前后加密采样、轨迹作为到点佐证）。

## License

[Apache-2.0](LICENSE)
