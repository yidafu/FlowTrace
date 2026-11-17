# 07 部署、可观测性与安全

> 上游：[01-总体设计](01-overview.md) §7 里程碑｜ 落地位置：`deploy/`

## 1. 部署拓扑

### 1.1 开发/联调（`deploy/`，`podman compose up -d`）

按 AGENTS.md 约定：**PostgreSQL 本地另行安装**，compose 只拉起中间件：

| 组件 | 形态 | 说明 |
|---|---|---|
| kafka | 容器：KRaft 单节点（`localhost:9092`） | 开发拓扑 |
| redis | 容器：redis:7（`localhost:6379`） | AOF everysec |
| postgres | **本地安装**（16） | 不入 compose |
| flowtrace-app | 本地构建（`./gradlew :services:flowtrace-app:bootRun`） | 唯一启动模块：五服务单进程（决策 D15） |

`examples/` 下 RN 示例 App 指向 `http://10.0.2.2`（Android 模拟器宿主映射）或局域网 IP。

### 1.2 生产（M3，k8s）

| 组件 | 形态 | 扩缩容 |
|---|---|---|
| flowtrace-app（多模块单体；容量需要时拆出 processor/realtime） | Deployment ×2+ | HPA（CPU 60%；upload 流量另以 QPS 指标） |
| processor（拆出后） | Deployment ×4 起 | HPA 自定义指标 `ft_processor_lag`（目标 < 2min），上限 = topic 分区数 |
| realtime（拆出后） | Deployment ×2 | 固定 + lag 告警 |
| PostgreSQL / Redis / Kafka | **托管服务优先**（RDS/云 Kafka 等价物）；自管则 StatefulSet + 副本 | — |

网络：服务间零 HTTP（06 §1），k8s NetworkPolicy 仅放行必要方向；PG/Redis/Kafka 不对公网。

## 2. 组件版本基线

PostgreSQL 16｜Redis 7（Lettuce）｜Kafka 3.x（KRaft）｜JDK 21（虚拟线程）｜Kotlin 2.3.21｜Spring Boot 4.1.2｜Exposed 1.0.0-beta-4（exposed-migration 管理 DDL）｜JTS 1.20｜SDK：RN 0.7x、`@bufbuild/protobuf`、pako 2。

## 3. 配置项总表（application.yaml，默认值即前文口径）

配置分两层，**以 `application.yaml` 为主口径**（默认值跟代码走，可按 profile 区分 dev/prod）：

- **基础设施**对齐 Spring Boot 标准键（不自行发明 `DB_URL` 类短名）；
- **业务参数**统一命名空间 `flowtrace.<service>.*`，各服务库模块以 `@ConfigurationProperties(prefix = "flowtrace.<service>")` 绑定，`flowtrace-app` 一份 `application.yaml` 汇总（下表即其骨架契约）。原 `CONSUMER_GROUP` 平铺名在五服务单进程下会撞名，按服务分键解决。

**覆盖规则**（Spring Boot relaxed binding，12-factor 能力不变）：

- 所有键均可被环境变量覆盖：`flowtrace.upload.topic` → `FLOWTRACE_UPLOAD_TOPIC`（大写、`.`→`_`、**连字符去掉**，如 `ts-window-rt-past-ms` → `FLOWTRACE_UPLOAD_TSWINDOWRTPASTMS`，故长键名不作部署期覆盖对象，只覆盖基础设施与少量开关）。
- 敏感项（datasource 凭据、对象存储）在 yml 中以 `${ENV_NAME:默认值}` 占位符引用部署注入的环境变量（沿用原短名），**不在 yml 写死**（§7 密钥管理）。
- dev 环境中间件地址走占位符默认值（`localhost`），生产由 env/ConfigMap 覆盖。

**全局（Spring Boot 标准键 + `flowtrace.auth`）**：

```yaml
spring:
  datasource:
    url: ${DB_URL:jdbc:postgresql://localhost:5432/flowtrace?reWriteBatchedInserts=true}
    username: ${DB_USER:flowtrace}
    password: ${DB_PASSWORD:}      # 凭据不写死，部署期经 Secret 注入
    hikari:
      maximum-pool-size: 10        # 原 DB_POOL_MAX（Hikari 默认）
  data:
    redis:
      url: ${REDIS_URL:redis://localhost:6379}
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP:localhost:9092}
logging:
  level:
    root: ${LOG_LEVEL:info}
    dev.yidafu.flowtrace: info
management:
  tracing:
    sampling:
      probability: 0.1             # 入口 10% + 错误全采（§5）
  otlp:
    tracing:
      endpoint: ${OTEL_ENDPOINT:http://localhost:4318/v1/traces}
flowtrace:
  auth:
    jwks-issuer:                   # 原 JWKS_ISSUER，部署期必填
    jwks-cache-ttl-sec: 300        # 原 JWKS_CACHE_TTL_SEC
```

**flowtrace-upload**（`flowtrace.upload.*`）：

```yaml
flowtrace:
  upload:
    topic: trace.batch.v1
    limit-batch-per-min-rt: 12        # 实时通道 批次/分钟
    limit-batch-per-min-bf: 6         # 补传通道 批次/分钟
    limit-points-per-min: 6000
    degrade-threshold-pct: 90
    degrade-windows: 10
    degraded-factor: 2
    payload-max-bytes: 262144         # 256KB
    gunzip-max-bytes: 2097152
    ts-window-rt-past-ms: 172800000   # 2d
    ts-window-bf-past-ms: 604800000   # 7d
    ts-window-future-ms: 600000       # 10min
    interval-base-sec: 30
    interval-high-sec: 60
    interval-veryhigh-sec: 120
    interval-limited-sec: 300
    interval-min-sec: 10
    interval-max-sec: 300
```

**flowtrace-processor**（`flowtrace.processor.*`）：

```yaml
flowtrace:
  processor:
    consumer-group: processor-group   # 原 CONSUMER_GROUP（与 realtime 分键，避免单体撞名）
    concurrency: 32                   # 默认 = topic 分区数（§1.2 M3 基线 32）
    poll-records: 100
    watermark-drift-ms: 600000        # 10min 乱序水位窗口
    clean-accuracy-cm: 10000
    clean-accuracy-lone-ms: 600000
    clean-speed-max-mps: 85
    split-gap-ms: 600000
    split-jump-m: 5000
    stay-radius-m: 50
    stay-min-ms: 300000
    stay-merge-gap-ms: 120000
    dp-storage-epsilon-m: 5
    points-write-batch: 1000
    dlq-max-retries: 3
    recalc-lock-ttl-sec: 300
    dirty-scan-interval-sec: 60
```

**flowtrace-realtime**（`flowtrace.realtime.*`）：

```yaml
flowtrace:
  realtime:
    consumer-group: realtime-group
    pos-ttl-days: 30
```

**flowtrace-query**（`flowtrace.query.*`）：

```yaml
flowtrace:
  query:
    span-max-traces-days: 7
    span-max-segments-days: 31
    span-max-distance-days: 31
    span-max-playback-days: 1
    limit-points-default: 2000
    limit-points-max: 5000
    location-stale-ms: 300000
```

**flowtrace-admin**（`flowtrace.admin.*`）：

```yaml
flowtrace:
  admin:
    partition-precreate-days: 3
    partition-cron: "0 5 0 * * *"
    archive-enabled: true
    archive-bucket: ${ARCHIVE_BUCKET:}  # 对象存储凭据经 Secret 注入
    retention-point-days: 30            # 全局默认，可被 USER 策略覆盖
    retention-segment-days: 180
    retention-archive-days: 365
```

## 4. 指标与告警（Micrometer → Prometheus）

| 指标 | 类型/标签 | 告警（默认） |
|---|---|---|
| `ft_upload_requests_total{code}` | Counter | 5xx 占比 > 1% 持续 5min → P1 |
| `ft_upload_duration_seconds` | Timer（p99） | p99 > 100ms 持续 5min → P2 |
| `ft_upload_dup_batches_total` / `ft_ratelimit_hits_total{channel}` | Counter | 限流命中突增（环比 ×10）→ P2（疑似异常设备/版本回归） |
| `ft_kafka_send_failures_total` | Counter | > 0 持续 1min → P1 |
| `ft_processor_lag`（consumer group lag） | Gauge | > 5min 滞后 → P1；> 2min → P2 |
| `ft_processor_points_total{result=inserted\|dropped, reason}` | Counter | 单一 reason 占比 > 5% → P2（清洗规则回归） |
| `ft_realtime_e2e_seconds`（received→Redis） | Timer | p95 > 30s → P2 |
| `ft_query_duration_seconds{endpoint}` | Timer | p95 > 500ms 持续 10min → P2 |
| `ft_dirty_day_pending` / `ft_dirty_day_failed` | Gauge | failed > 0 → P2；pending 持续增长 → P3 |
| `ft_dlq_total` | Counter | > 0/hour → P2 |
| `ft_partition_default_rows` | Gauge | > 0 → P2（时间越界数据出现） |
| `ft_archive_runs_total{result}` / `ft_delete_jobs_total{result}` | Counter | failed > 0 → P2 |
| 基础设施 | — | PG 磁盘 > 75% → P1；Redis 内存 > 80% → P2；证书 30 天到期 → P3 |

## 5. 日志与追踪

- 结构化 JSON：`ts, level, service, logger, msg, traceId, spanId`；业务字段只带 `deviceId/batchId/userId` 等 ID——**日志禁止输出坐标明文**（PIE 数据，隐私约定）。
- 追踪：OpenTelemetry（OTLP 导出），W3C traceparent；采样：入口 10% + 错误全采；`X-FlowTrace-Trace-Id` 回显客户端（04 §1）。
- 慢查询：PG `log_min_duration_statement=500ms` 落库审计；processor 批处理 > 5s 打 WARN。

## 6. 运维手册要点

| 场景 | 操作 |
|---|---|
| Kafka 分区扩容（M3 32→64） | 按序：确认 lag 正常 → 扩分区 → processor 滚动重启触发再平衡 → 观察水位窗口（10min）内乱序指标；期间**暂停** DLQ 重放/重处理作业（避免同设备跨分区乱序放大） |
| Redis 故障 | upload/processor 按 06 §2.6/§3 降级运行（DB 判重 + 本地令牌桶）；恢复后无需人工修复（幂等键/水位自然重建，水位兜底从 `(device_id, ts)` 索引 MAX 恢复） |
| PG 故障 | processor 暂停消费（不 commit、不 DLQ）；恢复后从 offset 续跑，幂等吸收重复 |
| 时间越界数据（default 分区行） | 校准时钟 → 数据搬运脚本（`INSERT INTO 正式分区 SELECT …` + 删除）→ 复位告警 |
| 保留策略变更 | admin 修改 `retention_policy` → 下次归档作业生效；缩短保留会触发大批量归档/drop，建议分批调整 |
| DLQ 处理 | 查 `GET /v1/admin/dlq/batches` → 修复（多为协议/数据 bug）→ 重放 → 观察 `deduplicated` 率 |
| 回滚 | 镜像回滚 + topic/表结构向前兼容（02 §1 只增约定保证旧版本可读新数据结构） |

## 7. 安全

| 域 | 措施 |
|---|---|
| 认证 | JWT RS256（外部 IdP 签发，服务仅持 JWKS 公钥）；scope 矩阵见 04 §1.5 |
| 传输 | 全链路 TLS 1.2+（网关终止）；内部网络隔离（NetworkPolicy/安全组） |
| 密钥 | DB/Redis/Kafka/对象存储凭据走 Secret 管理（k8s Secret / 云 KMS），不落镜像与代码库 |
| 数据保护 | 坐标为敏感位置数据：日志不出坐标；对象存储启用 SSE；备份/PITR 保留与删除限制已在 03 §10 明示（合规文档同步声明） |
| 审计 | 全部管理动作 + 删除/重处理/归档作业写 `audit_log`（03 §5），保留 400 天 |
| 防滥用 | 按设备限流 + DEGRADED 降级（06 §2.4）；异常设备告警人工处置（SUSPENDED/DISABLED） |
| 依赖 | CI 跑依赖漏洞扫描（如 dependency-submission / trivy 镜像扫描） |

## 8. 压测方案（M3 验收，01 §7）

| 场景 | 负载 | 通过标准 |
|---|---|---|
| 接入压测 | k6：1200 rps 混合批（实时 90%/补传 10%），30min | upload p99 < 100ms；零 5xx（除注入）；幂等率（重复注入 1% 样本 100% 去重） |
| 管道压测 | 合成轨迹灌 `trace.batch.v1` 12k 点/s 持续 1h（≈10 亿/天均值） | lag < 5min；点零重复（对账脚本：灌入计数 vs `trace_point` 计数）；段产物抽样人工比对 |
| 查询压测 | 200 rps 混合窗口（1h/1d/7d） | P95 < 500ms；无超时 |
| 故障演练 | kill processor（再平衡）、Redis failover、PG 重启 | 数据零丢失；恢复后自动续跑；告警按预期触发 |
| 数据合成 | 100 万虚拟设备 × 模式混合（静止/步行/驾车/停留）轨迹生成器，含 1% 乱序、0.5% 重复、0.1% 坏点 | 清洗丢弃率与注入率一致 |

## 9. 上线检查清单（MVP）

- [ ] golden vectors：TS/Kotlin 双端 CI 全绿（02 §8）
- [ ] PartitionManager：预建分区存在、default 分区为空、告警链路通
- [ ] JWKS 联调：真实 IdP token 通过；过期/错 scope 拒绝
- [ ] 限流与调速：阈值演练（单设备打满 → 429 → DEGRADED → 恢复）
- [ ] 删除演练：全存储矩阵覆盖 + 审计落库 + 在途 Kafka 拦截（墓碑）
- [ ] DLQ 演练：注入坏批 → DLQ → 重放幂等
- [ ] 告警：P1/P2 全通道（电话/IM）验证
- [ ] 回滚：上一镜像 + 表结构向前兼容验证
- [ ] 容量水位：PG 磁盘、Kafka 磁盘、Redis 内存基线记录并设阈值
