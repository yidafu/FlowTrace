# 06 服务端详细设计

> 上游：[01-总体设计](01-overview.md)、[02-协议](02-protocol.md)、[03-存储](03-storage.md)、[04-API](04-api.md)｜ 落地位置：`backend/libs/*`、`backend/services/*`

## 1. 共享库设计

依赖方向：`services → libs → platform`；服务之间**零 HTTP 调用**，只通过 Kafka/Redis/PostgreSQL 协作。服务模块均为库模块（无独立启动类），由 `flowtrace-app` 统一装配（决策 D15）。

### 1.1 `libs/trace-model`

- 领域对象：`TracePoint`（全 int 量化字段）、`TraceSegment`、`TraceBatch`、枚举（`Channel`/`MoveMode`/`SegmentKind`/`LocationSource`）。
- `GeoMath`（唯一几何工具，全服务复用；JTS 只做拓扑与 DP，决策 D7）：

| 函数 | 语义 |
|---|---|
| `quantize(deg) = round(deg × 1e7)` / `dequantize(q)` | 与 02 §2 一致（`Math.round` 语义） |
| `haversineM(a, b)` | 地球半径 R=6,371,008.8m |
| `initialBearingDeg(a, b)` | 初始方位角 |
| `toLocalMeters(origin, points)` / `fromLocalMeters` | 等距圆柱局部投影（equirectangular，`x = dLng·cos(originLat)·R`），供 DP 与 bbox 判断使用**米制**距离 |
| `dpSimplifyMeters(points, epsilonM)` | 局部投影 → JTS `DouglasPeuckerSimplifier` 等价实现（JTS TopologyPreservingSimplifier 不适用线串抽稀，直接用 JTS `DouglasPeuckerSimplifier` 于投影坐标）→ 反投影 |

### 1.2 `libs/trace-protocol`

- `TraceBatchRequestDecoder`：gzip 解压（上限 2MB）→ protobuf 解析 → Delta 解码（02 §4.3 伪码）→ 输出 `DecodedBatch(header, List<TracePoint>)`。
- `TraceBatchMessageCodec`：内部消息（02 §6）编解码。
- 校验器：§3.2 规则表的可复用实现（upload 调用）。
- 测试：golden vectors（`proto/testdata/v1` 挂为 test resources：Gradle `sourceSets.test.resources.srcDir("../../proto/testdata")` 类配置）；`pointId` 派生断言。

### 1.3 `libs/trace-messaging`

- `Topics`：`trace.batch.v1` / `trace.batch.dlq.v1`（拓扑见 §8）。
- Producer 封装：`enable.idempotence=true, acks=all, compression=lz4, linger.ms=5`；send 失败同步抛出（upload 返回 5xx 让 SDK 退避，不丢批）。
- Consumer 工厂：手动 ack（批处理成功后 commit offset，at-least-once + 下游幂等）、重平衡监听（日志 + 指标）、DLQ 发布器（携带 `error.code` 消息头）。

### 1.4 `libs/trace-storage/storage-postgres`

- Exposed `Table` 对象：03 文档 §2~§5 的表定义单一事实来源；分区表读写映射与非分区表一致，主/子表 DDL 走 `PartitionManager`（03 §7）。
- DDL 变更：非分区表走 exposed-migration（脚本由表定义差异生成、随代码入库，03 §1；决策 D16）。
- `PointsWriter.batchInsertIgnore(points)`：JDBC prepared batch（1000/批）+ `ON CONFLICT (ts, point_id) DO NOTHING` + `reWriteBatchedInserts=true`；返回实际插入数（供指标）。
- `KeysetPager`：`(user_id, ts, point_id)` 游标分页 helper（04 §1 cursor 编解码）。
- 连接池：Hikari `pool = 2 × cores`，`maximumPoolSize ≤ (PG max_connections − 运维保留) / 实例数`。

### 1.5 `libs/trace-storage/storage-redis`

封装 03 §8 全部 key：`IdempotencyGate.tryAcquire(batchId)`（SETNX）、`RateLimiter.check(deviceId)`（Lua INCR+PEXPIRE）、`LatestPositionStore.writeIfNewer`（Lua：入参 ts > 现 ts 才 HSET）、`TombstoneStore`、`WatermarkStore.maxSeenTs(deviceId)`。Redis 不可用时的降级行为逐封装定义（§3.6）。

## 2. flowtrace-upload

### 2.1 职责

校验、幂等、限流、写 Kafka、快速返回（含动态调速）。**不落轨迹数据**、不做重处理。

### 2.2 处理链（`POST /v1/traces/batch`）

```text
1  JWT 校验（platform/common；JWKS 缓存 5min；需 scope=trace:write 且 did 非空）
2  设备状态检查：本地缓存（Caffeine, TTL 60s）→ 不在缓存查 device 表 →
   DISABLED/SUSPENDED → 403 DEVICE_DISABLED
3  限流：RateLimiter.check（§2.4）→ 超限 429
4  读取 body（≤256KB）→ gunzip（≤2MB）→ protobuf 解析 → Delta 解码   [trace-protocol]
5  字段校验（§2.3 规则表）
6  批内折叠：相邻 (ts,lat_q,lng_q) 完全相同的点合并（accepted 数相应减少）
7  幂等：IdempotencyGate.tryAcquire(batchId)
     - 命中 → 200 { deduplicated: true }（不写 Kafka，流程结束）
8  写 Kafka（key=deviceId，TraceBatchMessage 绝对值）send().get()（同步确认）
9  异步（内存队列→单独线程池）：INSERT trace_batch ACCEPTED（冲突=重复批，忽略）
     Redis SETNX 失败窗口的兜底：INSERT 冲突时若 Kafka 未写（第 8 步之前不可达，此路径仅在
     Redis 丢失后重放场景出现）→ 依 batch 已存在判定重复，丢弃消息（记指标）
10 响应 200 { accepted, nextUploadIntervalSec(§2.5) }
```

### 2.3 字段校验规则表

| 项 | 规则 | 失败码 |
|---|---|---|
| `schema_version` | `1.x` | `INVALID_SCHEMA_VERSION` |
| `device_id` / `user_id` | 非空且 == token `did` / `sub` | `FORBIDDEN` |
| `batch_id` | 合法 UUID | `PROTOCOL_ERROR` |
| `channel` | 1 或 2 | `PROTOCOL_ERROR` |
| 点数 | REALTIME ≤ 100；BACKFILL ≤ 500 | `PAYLOAD_TOO_LARGE` |
| Delta `d_ts` | ≥ 0（解码器内联校验） | `TS_NOT_SORTED` |
| 绝对坐标 | ±9e8 / ±1.8e9 | `INVALID_COORD` |
| 点 ts 窗口 | REALTIME：`[now−48h, now+10min]`；BACKFILL：`[now−7d, now+10min]` | `TS_OUT_OF_WINDOW` |
| 未知 flags 位 | 忽略（前向兼容） | — |
| `speed_mps`/`accuracy_cm` 越界 | 不拒收（processor 清洗处理） | — |

### 2.4 限流（按设备）

默认阈值（配置项见 07）：批次数 `12/min`（REALTIME；BACKFILL 独立计数 `6/min`）、点数 `6000/min`。Lua：`INCR key; if n==1 PEXPIRE 60s; return n`，`n > limit` 即 429（本分钟后续请求持续 429，SDK 被迫降速）。**降级机制**：设备连续 10 个分钟窗口点数 ≥ 90% 阈值 → `device.status=DEGRADED`（写库 + 事件）→ 该设备阈值减半；`DEGRADED` 持续 24h 无触发自动回 `ACTIVE`（admin 定时任务）。

### 2.5 动态调速

按 04 §2.3 算法实现于响应组装处；`cfg:upint` 运维覆盖优先。

### 2.6 失败路径

| 故障 | 行为 |
|---|---|
| Redis 不可用 | 幂等降级为「仅 trace_batch 唯一索引」判重（第 7 步改为先查后插，耗时增加仍满足 p99 目标的 80% 场景）；限流降级为「本地令牌桶（单实例近似）」+ 告警 |
| Kafka send 失败/超时 | 5xx（SDK 退避重试同批）；**绝不**先幂等后写失败地返回 200 |
| 解压/解码/校验失败 | 400/413/422（确定性失败，SDK 放弃该批）；异常批样本（首 1KB）打点采样供排查 |

## 3. flowtrace-processor

### 3.1 消费配置

`processor-group`；并发 = 分区数；`max.poll.records=100`；**批处理成功后 commit**（at-least-once，下游全幂等）；DB 不可达时不 commit、暂停该分区重试（避免 DLQ 风暴）；单批处理超时 60s → 重投 DLQ。

### 3.2 管道（每条 `TraceBatchMessage`）

```text
1  解码消息（内部 proto）
2  墓碑检查：TombstoneStore.has(userId) → 是则丢弃整批（记指标 drop_tombstone），批状态置 FAILED(error=TOMBSTONE)
3  水位更新：wm = WatermarkStore.maxSeenTs(deviceId)；lateBatch = 消息内最小 ts < wm − 10min
   WatermarkStore.writeIfGreater(deviceId, 消息最大 ts)
4  清洗（§3.3 规则表，逐点）
5  幂等写点：PointsWriter.batchInsertIgnore → inserted/dropped 计数
6  if lateBatch:  对批覆盖的每个 (userId, day)：INSERT dirty_day ON CONFLICT DO NOTHING
                 跳过 7（在线分段）；继续下一批
7  在线分段 + 停留点 + 抽稀（§3.4/§3.5）→ upsert trace_segment
8  UPDATE trace_batch status=PROCESSED, processed_at=now（该写走异步缓冲，非热路径）
```

### 3.3 清洗规则表（默认值可配，07）

| 规则 | 默认阈值 | 动作 | 指标 tag |
|---|---|---|---|
| 零岛 | `lat_q==0 && lng_q==0` | 丢点 | `zero` |
| 精度过差 | accuracy > 100m **且** 距上一保留点 < 10min | 丢点 | `accuracy` |
| 速度异常 | 与上一保留点隐含速度 > 85 m/s | 丢当前点 | `speed` |
| 完全重复 | 与上一保留点 `(ts, lat_q, lng_q)` 相同 | 丢点 | `duplicate` |
| 未来时间 | ts > now + 10min | 丢点 | `future` |

（点先经 5 幂等写入，「丢」指清洗过滤不入库；规则在写库前应用——清洗即过滤后写入。）

### 3.4 分段算法（在线）

维护每设备 OPEN 段（consumer 内存 LRU，miss 时查 `(user_id, status=1)` 索引恢复）：

```text
split(points):                        # 清洗后按 ts 升序
  for p in points:
    if openSeg != null:
      gapMs = p.ts − openSeg.last.ts
      jumpM = haversineM(p, openSeg.last)
      if gapMs > 10min or jumpM > 5000 or confirmedMode(p) != openSeg.moveMode:
        closeSegment(openSeg)         # 定界判定 + upsert
        openSeg = null
    openSeg.add(p)
closeSegment(seg):
  stays = detectStay(seg.points)      # §3.5，产出 STAY 段与 MOVE 子段区间
  for 每个区间 kind∈{STAY, MOVE}:
    segmentId = UUIDv5(SEGMENT_NS, "${userId}:${sessionId}:${startTsMs}")   # D8
    distance = Σ haversine（MOVE；STAY=0）
    geometry  = STAY: POINT(质心)；MOVE: WKT LINESTRING(dpSimplifyMeters(pts, 5m))
    upsert trace_segment（同 segmentId → 更新 end_ts/point_count/distance/geometry/bbox/updated_at；
                          插入时 status=OPEN）
  段 end_ts < wm − 10min → status=FINALIZED（收尾时顺带判定；另有定时扫描兜底）
```

- `confirmedMode`：点 `flags&MOTION_CHANGE` 或按点速度推断（<0.5m/s STILL，0.5~3 WALK，3~8 RUN，8~15 BICYCLE，>15 VEHICLE），段模式取区间多数投票。
- **段扩展幂等**：后续点延长 OPEN 段时 `start_ts` 不变 → `segmentId` 不变 → upsert 更新而非新增。

### 3.5 停留点（STAY）算法

```text
detectStay(points):                   # 双指针，O(n)
  i = 0
  while i < n:
    j = i
    while j+1 < n and haversineM(points[j+1], points[i]) <= 50m: j += 1
    if points[j].ts − points[i].ts >= 5min:
      emit STAY(质心(points[i..j]), start=points[i].ts, end=points[j].ts)
      与上一个 STAY 间隔 ≤ 2min → 合并（重算质心/时长）
      i = j + 1
    else:
      i += 1
  未纳入 STAY 的区间 → MOVE 子段
```

默认参数：`stayRadiusM=50, stayMinMs=300_000, stayMergeGapMs=120_000`（可配）。

### 3.6 迟到重算（D13）

`dirty_day` 消费循环（processor 内定时线程，`SELECT ... FOR UPDATE SKIP LOCKED LIMIT 10`）：

```text
重算 (userId, day):
  读该日 ±1h 范围内全部 trace_point（含前日尾部，保证段衔接）
  删除该日范围的 trace_segment（segmentId 派生自 start_ts，删除范围与重建范围一致）
  按 §3.4 离线重跑（无 OPEN 状态，直接 FINALIZED；与相邻日段的拼接误差 ≤ 边界 1 段，接受）
  dirty_day.status=DONE；失败 attempts+1，≥3 次 FAILED（告警，人工介入）
```

运行期间该用户在线管道可能并发写段：重算前先记该用户 `recalc:u:{userId}` 锁（Redis SETNX 5min），在线管道遇锁则该批只写点不分段、再入 dirty_day——保证串行化（风险 R5）。

### 3.7 DLQ

进入条件：单批处理重试 3 次失败（DB 故障类**不进** DLQ，阻塞重试）；消息解析失败（理论不可达，防御）。动作：原消息 + `error.code` 头投 `trace.batch.dlq.v1`；`trace_batch.status=DLQ`。重放：admin `POST /v1/admin/reprocess`（type=dlq）从 DLQ 读出重投主 topic——batchId 幂等天然防重。

## 4. flowtrace-realtime

- 消费 `trace.batch.v1`（`realtime-group`），取批内 `ts` 最大的点 → `LatestPositionStore.writeIfNewer`（`rt:pos:u:{userId}` 与 `rt:pos:d:{deviceId}`，TTL 30d）——乱序到达（补传旧批）不会回退最新位置。
- 无独立 API（`GET /v1/users/{id}/location` 由 flowtrace-query 读 Redis 提供）。
- 端到端延迟指标：`received_ts_ms` 与 Redis 写入时刻差值（验证 ≤30s 目标）；消费 lag 告警（07 §4）。

## 5. flowtrace-query

### 5.1 执行计划（各接口 SQL 形态）

| 接口 | 主查询 |
|---|---|
| `GET /v1/traces` | `SELECT ts, point_id, lat_q, lng_q, … FROM trace_point WHERE user_id=? AND ts>=? AND ts<? AND (ts,point_id)>(cursor…) ORDER BY ts LIMIT ?`（keyset；`simplify=false`） |
| `GET /v1/trace-segments` | `SELECT … FROM trace_segment WHERE user_id=? AND start_ts>=? AND start_ts<? [AND kind=?] [AND session_id/task_id…] ORDER BY start_ts LIMIT ?` |
| `GET /v1/traces/playback` | 同 `/v1/traces` + 简化管道 |
| `GET /v1/traces/distance` | `SELECT kind, move_mode, sum(distance_m) FROM trace_segment WHERE user_id=? AND start_ts>=? AND start_ts<? AND kind=1 GROUP BY move_mode`（存在 OPEN 段或缺段 → 回退点级 haversine，响应 `source=points`） |
| `GET /v1/users/{id}/location` | Redis `rt:pos:u:{id}` 单读 |

跨分区说明：查询窗口跨多天分区时 PG 按分区裁剪 + Append；`limit` 以首批满足为准（keyset 保证正确性）。

### 5.2 简化 LUT（zoom → DP ε）

| zoom | ≤10 | 11–12 | 13–14 | 15–16 | ≥17 |
|---|---|---|---|---|---|
| ε | 100m | 50m | 20m | 10m | 3m |

存储几何固定 ε=5m；查询在**存储几何之上**再做 DP（点查询则对原始点直接 DP）。简化后仍超 `maxPoints` → 等距抽取兜底（保首尾点）。

### 5.3 响应组装

- `format=geojson`：LineString + 平行 `timestampsMs` 数组（时间不内嵌坐标）。
- `format=compact`：`[[tsMs, latE7, lngE7], …]`。
- bbox 直接取段表冗余列，不解析 WKT。

### 5.4 P95 <500ms 预算（1 天窗口典型查询）

| 步骤 | 预算 |
|---|---|
| 网关/TLS/鉴权（JWKS 已缓存） | 25ms |
| 点/段查询（索引扫描 ≤5000 行） | 150ms |
| DP 简化（投影 + 抽稀） | 50ms |
| 序列化 + 传输 | 100ms |
| **合计典型** | **~325ms**（P95 目标内） |

## 6. flowtrace-admin

| 职责 | 实现 |
|---|---|
| 设备管理 | 04 §5 API；状态变更后主动失效 upload 的本地缓存（写 `cfg:devver:{deviceId}` 版本号，upload 缓存按版本失效——避免等 60s TTL） |
| 保留策略 | CRUD + 生效（归档/删除作业读取） |
| PartitionManager 调度 | 每日 00:05 + 启动时（03 §7） |
| 归档作业 | 到期分区（`ts < now − hotDays − 1d` 缓冲）→ 临时 Parquet 对象 → 行数/sha256 校验 → manifest → `DROP PARTITION` → 审计；失败保分区 + 告警；作业幂等（manifest 已存在则跳过写直接 drop） |
| 删除作业 | 03 §10 矩阵执行；`jobs` 表（jobId/type/status/progress jsonb/error）支撑 04 §4 查询；逐分区独立事务、断点续跑 |
| 重处理 | `user-day`：写 `dirty_day`（reason=MANUAL）；`dlq`：DLQ 重投（§3.7） |
| 审计 | 所有管理动作写 `audit_log`（03 §5） |

## 7. Kafka 拓扑与生产/消费参数

| topic | 分区 | key | value | retention | cleanup | 生产者 | 消费组 |
|---|---|---|---|---|---|---|---|
| `trace.batch.v1` | 32（M3 扩 64） | deviceId | `TraceBatchMessage`（lz4，≤1MB） | 72h | delete | flowtrace-upload | `processor-group`、`realtime-group` |
| `trace.batch.dlq.v1` | 8 | deviceId | 原消息 + error 头 | 7d | delete | flowtrace-processor | 无常驻（admin 重放时读） |

- 同设备有序（key 分区）；批间乱序由水位机制吸收（§3.2）。
- 分区扩容流程：新分区仅接收新哈希分布 → 短暂同设备跨分区乱序 → 水位 10min 窗口内自动吸收；扩容时暂停补传类重放操作（07 §6）。

## 8. 多模块单体启动模块（flowtrace-app，决策 D15）

`services/flowtrace-app` 为唯一启动模块：聚合全部五个服务库模块与一份 `application.yml`（`@SpringBootApplication` 扫 `com.flowtrace` 全包）；**模块边界不变**（独立 Gradle 模块、包名 `com.flowtrace.{upload,processor,query,realtime,admin}`，禁止跨包 import 实现类）。拆分部署时只加启动模块，不改库模块代码——容量需要时优先拆出 `flowtrace-processor`、`flowtrace-realtime`。

## 9. 服务端测试要点

| 层 | 内容 |
|---|---|
| trace-protocol | golden vectors 全量；量化边界（±9e8/±1.8e9）、d_ts=0、跨日界线 Delta |
| upload | 规则表逐条参数化测试；幂等并发（同 batchId 100 并发只放行 1）；Redis 降级路径；Lua 限流窗口边界 |
| processor | 属性测试：随机轨迹注入乱序/重复/迟到/坏点 → 不变量断言（点零重复入库、segmentId 确定性、重放结果幂等）；停留点用例表（进入-停留-离开 / 漂移边缘 / 相邻合并）；水位跨重启恢复 |
| query | LUT 简化前后误差上界断言（DP ε 保证）；keyset 分页深翻页一致性；里程段/点两口径一致性 |
| 集成 | podman compose（Kafka/Redis）+ 本地 PG + 全服务冒烟：端到端延迟、删除矩阵、DLQ 重放、分区预建/归档/drop 全流程 |
