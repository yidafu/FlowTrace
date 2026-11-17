# FlowTrace 详细设计文档

> 状态：v1.1（2026-11-17，与工程骨架同步：多模块单体 flowtrace-app、platform 四合一、Exposed migration 管理 DDL，新增决策 D15/D16）｜ 读者：参与 FlowTrace 实现的后端 / SDK / 测试 / 运维工程师
> 上游文档：[AGENTS.md](../../AGENTS.md)（硬性约束与架构边界，优先级最高）、[README.md](../../README.md)（项目概览）
> 本目录文档的目标：**达到可直接参考实现的程度**——协议、表结构、API、算法、状态机、配置、指标均给出可执行级别的定义。

## 文档地图（建议阅读顺序）

| # | 文档 | 内容 | 主要读者 |
|---|---|---|---|
| 1 | [01-总体设计](01-overview.md) | 目标指标、总体架构、模块职责、关键技术决策记录（ADR）、端到端时序、容量估算、里程碑、风险 | 全体 |
| 2 | [02-协议与编码设计](02-protocol.md) | proto 完整定义、Delta 编码规范、GZIP 约定、ID 派生与幂等语义、错误映射、一致性测试向量 | 后端 / SDK |
| 3 | [03-存储与数据模型](03-storage.md) | PostgreSQL 表结构/索引/分区/保留、Redis key 设计、Parquet 归档布局、数据删除矩阵 | 后端 / 运维 |
| 4 | [04-API 设计](04-api.md) | 全部 REST 接口契约（请求/响应/错误/分页/限流/动态调速算法） | 后端 / SDK / 业务方 |
| 5 | [05-SDK 详细设计](05-sdk.md) | RN 库工程结构、公开 API、采样状态机、本地队列 schema、双通道上报、后台执行矩阵、轨迹管理页面（§9）、隐私 | SDK / 业务方 |
| 6 | [06-服务端详细设计](06-backend-services.md) | 公共库与五个服务的内部设计：校验规则、处理管道、算法伪码、Kafka 拓扑、幂等实现 | 后端 |
| 7 | [07-部署、可观测性与安全](07-deploy-observability.md) | 部署拓扑、配置项总表、指标与告警、日志与追踪、安全矩阵、压测方案、上线清单 | 后端 / 运维 |

## 术语表

| 术语 | 含义 |
|---|---|
| 轨迹点（point） | 端侧一次采样保留下的定位修复：`(ts, lat, lng, accuracy, speed, bearing, altitude, source, flags)` |
| 量化坐标（lat_q / lng_q） | 十进制度 ×10⁷ 后的 32 位整数，全链路（协议、存储、内存模型）统一使用 |
| 批（batch） | 一次上报的点的集合，由 `batchId`（UUIDv7）全局唯一标识 |
| 实时通道（realtime） | 小批（≤50 点）任意网络上报的通道，保证最新点端到端 ≤30s |
| 补传通道（backfill） | WiFi + 充电时大批（≤500 点 / ≤256KB）上传历史积压的通道 |
| 轨迹段（segment） | processor 产出的连续移动片段，`kind = MOVE / STAY`，geometry 存 WKT 文本 |
| 停留点（stay） | `kind = STAY` 的轨迹段：连续 ≥5min 且都在半径 50m 内的点簇质心 |
| 幂等键 | 批级 `batchId`、点级 `pointId = UUIDv5(batchId, idx)`（见 02 文档） |
| 水位（watermark） | 按设备计算的 `已见最大 ts − 10min`，用于段定界与迟到数据判定 |
| 补录 | 超过 10 分钟乱序窗口的迟到数据：正常入库 + 标记受影响日期重算段 |
| 抽稀 | Douglas-Peucker 化简；存储段几何用 ε=5m，查询按缩放级别用 LUT（见 06 文档） |
| 轨迹管理页面 | SDK 可选 UI 子入口 `./ui`（FlowTraceManagerScreen）：本地队列可视化、上报状态、手动补报、MapLibre 地图（见 05 §9） |
| 巡检业务 | 通过 `taskId / sessionId` 关联并消费轨迹结果的下游业务，不侵入上报主链路 |

## 文档约定

- 本目录所有 proto / SQL / Kotlin / TypeScript 代码块**均为设计规格**，落地时生成到对应模块（`proto/`、`backend/**`、`sdk/**`），不在文档外的空白处随手实现。
- 参数默认值统一写成 `key = 默认值（配置项名）` 形式，配置项总表见 07 文档。
- 标注【决策 Dx】的条目对应 [01-总体设计 §6](01-overview.md) 的决策记录；标注【风险】的条目对应 §10。
- 日期与时间戳：网络传输一律 epoch 毫秒（int64）；HTTP 查询参数用 ISO-8601 UTC；存储用 `timestamptz`。
