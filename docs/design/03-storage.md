# 03 存储与数据模型

> 上游：[01-总体设计](01-overview.md)（决策 D1、D5~D9、D13、D14）｜ 落地位置：`backend/libs/trace-storage/storage-postgres`、`storage-redis`、flowtrace-admin 归档作业

## 1. 总览

| 存储 | 承载 | 保留（默认） |
|---|---|---|
| PostgreSQL 16（普通表，无 PostGIS） | `trace_point` / `trace_segment` / `trace_batch` 三张核心表 + admin 域表 | 点 30 天热、段 180 天热、批 7 天 |
| Redis 7 | 最新位置、批幂等、限流计数、删除墓碑 | 60s ~ 30d 不等（见 §8） |
| 对象存储（S3 兼容） | 冷数据 Parquet | 365 天（策略可配 180~730） |

通用约定：

- 时间列一律 `timestamptz`（UTC）；ID 一律 `uuid`；量化坐标一律 `int4`。
- `user_id` 为 `int8`（业务系统数字 ID）。若业务方只有字符串 ID，由 flowtrace-admin 维护映射表，主链路不感知。
- **DDL 变更用 Exposed migration 管理**（AGENTS.md；决策 D16）：表定义（Exposed `Table` 对象）为唯一事实源，migration 脚本由表定义与库结构差异生成、随代码入库 review；不引入 Flyway/Liquibase。分区表的分区子表由 `PartitionManager` 运行时 DDL 管理（§7）。表结构变更约定见 01 §9 R7。

## 2. `trace_point`（原始轨迹点）

按天 RANGE 分区（`ts`），**原始点与轨迹段分表**。

| 列 | 类型 | 空 | 说明 |
|---|---|---|---|
| `ts` | timestamptz | NO | 点时间（分区键） |
| `point_id` | uuid | NO | `UUIDv5(FLOWTRACE_POINT_NS, "{batchId}:{idx}")`（02 §7） |
| `user_id` | int8 | NO | |
| `device_id` | varchar(64) | NO | |
| `batch_id` | uuid | NO | 溯源批 |
| `session_id` | varchar(64) | YES | |
| `task_id` | varchar(64) | YES | 业务关联，透传 |
| `lat_q` / `lng_q` | int4 | NO | ×10⁷ 量化 |
| `accuracy_cm` | int4 | YES | |
| `speed_mps` | float4 | YES | |
| `bearing_deg` | int2 | YES | 0..359 |
| `altitude_cm` | int4 | YES | |
| `source` | int2 | YES | LocationSource 数值 |
| `flags` | int4 | YES | 位掩码（02 §3），缺省 0 |
| `received_at` | timestamptz | NO | default now()，入库时刻 |

约束与索引：

```sql
-- 主键（分区表唯一约束必须含分区键；pointId 内嵌时间戳，重复点 ts 相同 → 等价全局防重）
PRIMARY KEY (ts, point_id)
-- 主查询路径：用户 + 时间范围
INDEX (user_id, ts)
-- 设备对账/诊断
INDEX (device_id, ts)
-- 会话查询（部分索引省空间）
INDEX (session_id, ts) WHERE session_id IS NOT NULL
```

- 行宽估算：数据 ≈100B + 索引 ≈120B → **≈220B/点**（01 §8 容量口径）。
- 写入路径：processor 批量 `INSERT ... ON CONFLICT (ts, point_id) DO NOTHING`（JDBC batch 1000，`reWriteBatchedInserts=true`）。
- 读取路径：`(user_id, ts)` keyset 分页（06 §5）；不做任何地理查询（空间计算在应用层）。

## 3. `trace_segment`（轨迹段，含停留点，决策 D6）

按月 RANGE 分区（`start_ts`）。抽稀后的派生数据，**可由 `trace_point` 全量重算**（重算/补录的根基，D8/D13）。

| 列 | 类型 | 空 | 说明 |
|---|---|---|---|
| `start_ts` | timestamptz | NO | 分区键 |
| `segment_id` | uuid | NO | `UUIDv5(FLOWTRACE_SEGMENT_NS, "{userId}:{sessionId}:{start_ts_ms}")`，确定性 → 幂等 upsert |
| `user_id` | int8 | NO | |
| `device_id` | varchar(64) | NO | |
| `session_id` | varchar(64) | YES | |
| `task_id` | varchar(64) | YES | |
| `kind` | int2 | NO | 1=MOVE（LINESTRING）/ 2=STAY（POINT，停留点） |
| `move_mode` | int2 | NO | 0 UNKNOWN / 1 STILL / 2 WALK / 3 RUN / 4 BICYCLE / 5 VEHICLE |
| `end_ts` | timestamptz | NO | |
| `point_count` | int4 | NO | 段内原始点数 |
| `distance_m` | float8 | NO | MOVE：haversine 里程（米）；STAY：0 |
| `geometry` | text | NO | WKT（MOVE 为抽稀 ε=5m 的 LINESTRING；STAY 为质心 POINT） |
| `stay_lat_q` / `stay_lng_q` | int4 | YES | STAY 质心冗余列（免解析 WKT） |
| `min_lat_q` / `max_lat_q` / `min_lng_q` / `max_lng_q` | int4 | NO | bbox，应用层粗筛用 |
| `status` | int2 | NO | 1=OPEN（水位之上，仍可能被新点扩展）/ 2=FINALIZED |
| `updated_at` | timestamptz | NO | |

索引：

```sql
PRIMARY KEY (start_ts, segment_id)
INDEX (user_id, start_ts)            -- 主查询：用户时间范围内的段
INDEX (session_id)    WHERE session_id IS NOT NULL
INDEX (task_id)       WHERE task_id IS NOT NULL
INDEX (user_id, status) WHERE status = 1   -- processor 查找设备/用户的 OPEN 段
```

- WKT 体量：DP ε=5m 后城市 10km 轨迹 ≈50~200 顶点 ≈1~5KB/段；bbox 四列让消费方（如巡检系统）不解析 WKT 即可粗筛。
- STAY 段 `point_count` 为参与停留判定的点数，`end_ts − start_ts` 为停留时长。

## 4. `trace_batch`（上报批次）

按天 RANGE 分区（`received_at`）。幂等审计与对账；非业务查询表。

| 列 | 类型 | 空 | 说明 |
|---|---|---|---|
| `received_at` | timestamptz | NO | 分区键 |
| `batch_id` | uuid | NO | 幂等键（唯一） |
| `user_id` | int8 / `device_id` | varchar(64) | NO | |
| `channel` | int2 | NO | 1 realtime / 2 backfill |
| `point_count` | int4 | NO | 申报点数 |
| `first_ts` / `last_ts` | timestamptz | NO | 批内点范围 |
| `client_ts` | timestamptz | NO | SDK 编码时刻（延迟分析） |
| `sdk_version` | varchar(32) | NO | |
| `platform` | varchar(16) | NO | |
| `status` | int2 | NO | 1 ACCEPTED / 2 PROCESSED / 3 FAILED / 4 DLQ |
| `error_code` | varchar(64) | YES | FAILED/DLQ 原因 |
| `processed_at` | timestamptz | YES | |

```sql
PRIMARY KEY (received_at, batch_id)
UNIQUE (received_at, batch_id)       -- 与主键等价，显式声明语义
INDEX (device_id, received_at)
INDEX (status) WHERE status IN (3, 4)   -- 失败批扫描（重处理入口）
```

幂等实现：热路径 Redis SETNX（06 §3.4）；本表唯一索引是兜底与审计（插入冲突 → 判定为重复批处理）。

## 5. admin 域表（非分区，Exposed migration 管理）

**`device`**

| 列 | 类型 | 说明 |
|---|---|---|
| `device_id` | varchar(64) PK | SDK 安装 ID |
| `user_id` | int8 NO | 归属用户 |
| `platform` / `app_version` / `sdk_version` | varchar | 最近一次上报刷新 |
| `status` | int2 NO | 1 ACTIVE / 2 DEGRADED（限流自动降级，06 §3.5）/ 3 SUSPENDED / 4 DISABLED |
| `last_seen_at` / `created_at` | timestamptz | |

索引：`(user_id)`。

**`retention_policy`**：`id uuid PK`、`scope_type int2`（1 GLOBAL / 2 USER）、`user_id int8 NULL`、`hot_point_days int4 默认 30`、`segment_hot_days int4 默认 180`、`archive_enabled bool 默认 true`、`archive_keep_days int4 默认 365`、`updated_at`。USER 记录覆盖 GLOBAL。

**`audit_log`**（按月分区）：`id uuid(v7)`、`actor varchar(128)`（userId 或 `system:job名`）、`action varchar(64)`（`USER_TRACES_DELETE` / `DEVICE_DISABLE` / `POLICY_UPDATE` / `REPROCESS` / `PARTITION_DROP` / `ARCHIVE_RUN` / `DLQ_REPLAY`）、`target varchar(128)`、`detail jsonb`、`created_at`。保留 400 天。

**`dirty_day`**（补录重算队列，D13）：`user_id int8`、`day date`、`reason int2`（1 LATE_DATA / 2 MANUAL）、`attempts int2 默认 0`、`status int2`（1 PENDING / 2 RUNNING / 3 DONE / 4 FAILED）、`first_seen_at`，`PRIMARY KEY (user_id, day)`。processor 写入，admin 重算任务消费（06 §4.6）。

## 6. 行宽与体量汇总

| 表 | 行宽（含索引） | 满负荷增速 | 默认热保留 |
|---|---|---|---|
| trace_point | ≈220B | ≈220GB/天（管道上限） | 30 天 |
| trace_segment | ≈1.5KB（含 WKT） | ≈点数/100 | 180 天 |
| trace_batch | ≈200B | ≈4GB/天 | 7 天 |
| audit_log / device / policy | 小 | 小 | 400 天 / 永久 |

## 7. PartitionManager（决策 D9）

位置：`storage-postgres` 提供 DDL 模板与执行器；flowtrace-admin 持有调度（每日 00:05 + 启动时）。

职责：

1. **预建分区**：`trace_point` / `trace_batch` 预建今天起未来 3 天；`trace_segment` / `audit_log` 预建未来 2 个月。
2. **兜底分区**：各分区主表建 `*_default` DEFAULT 分区，捕获越界时间戳（时钟漂移防丢数据）+ 触发告警 `partition_default_rows>0`；修复 = 校准后搬运数据。
3. **过期处理**：与归档作业协同（06 §7.3）——归档成功并校验行数后才 `DROP PARTITION`；归档失败保留分区并告警。

```sql
-- 模板（运行时 DDL，非 migration 文件；所有表名/日期由代码参数化生成）
CREATE TABLE IF NOT EXISTS trace_point_p20261118
  PARTITION OF trace_point FOR VALUES FROM ('2026-11-18 00:00:00+00') TO ('2026-11-19 00:00:00+00');
```

实现注意：

- Exposed 无分区 DDL 支持：**分区表的列/类型定义仍以 Exposed `Table` 对象为单一事实来源**（读写映射复用），但主表 `CREATE TABLE ... PARTITION BY RANGE (...)` 与子表 DDL 由 PartitionManager 按模板执行；非分区表走 exposed-migration 生成的脚本（决策 D16）。
- 索引在分区主表上声明，PG 自动下建子表；`PRIMARY KEY/UNIQUE` 必须包含分区键（已满足）。
- 每日预建成功写 `audit_log(action=PARTITION_CREATE)`（可聚合，避免噪音——实现时改为指标 + 周期汇总审计）。

## 8. Redis key 规范（`storage-redis` 统一封装）

| key | 类型 | TTL | 写者 | 说明 |
|---|---|---|---|---|
| `rt:pos:u:{userId}` | HASH：`ts,latQ,lngQ,deviceId,accuracyCm,speedMps,bearingDeg,updatedAt` | 30d | flowtrace-realtime | 用户最新位置（多设备取 ts 最大者） |
| `rt:pos:d:{deviceId}` | HASH 同上 | 30d | flowtrace-realtime | 设备最新位置 |
| `idem:batch:{batchId}` | STRING `"1"` | 7d | flowtrace-upload | 批幂等（SETNX，06 §3.4） |
| `rl:bc:{deviceId}` | STRING 计数 | 60s 滑窗 | flowtrace-upload | 每分钟批数（Lua 原子 INCR+PEXPIRE） |
| `rl:pc:{deviceId}` | STRING 计数 | 60s 滑窗 | flowtrace-upload | 每分钟点数 |
| `tomb:u:{userId}` | STRING jobId | 24h | flowtrace-admin | 删除墓碑，processor 写入前检查（D14） |
| `cfg:upint:{deviceId}` | STRING 秒数 | 10min | flowtrace-admin | 手动调速覆盖（运维手段，优先级最高） |

约束：所有 key 只经 `storage-redis` 封装访问（禁止裸拼 key）；hash 字段名即上表，扩展时只增不改。可选（P2）：`seg:cache:{userId}:{start}:{end}:{zoom}` 段查询缓存 TTL 60s。

## 9. Parquet 冷数据布局

```text
trace-archive/
  ub={userId % 100}/                # 目录分桶防热点
    u={userId}/
      date=2026-11-10/
        points.parquet               # 列：ts(int64 epoch ms), point_id, device_id, session_id,
                                     #     task_id, lat_q, lng_q, accuracy_cm, speed_mps,
                                     #     bearing_deg, altitude_cm, source, flags
                                     # 编码：zstd；行组 64MB；按 (ts) 排序写入
        segments.parquet             # 列同表（geometry 为 WKT string）
  _manifest/
    runs/2026-11-11T02-run42.json    # 归档运行：源分区、行数、对象键、sha256、耗时
    deletes/u={userId}.json          # 用户删除墓碑清单（对象键 + 删除时间）
```

- 写入：flowtrace-admin 归档作业（06 §7.3），读当日过期分区 → 写 Parquet → 行数校验 → drop 分区。
- 读取（P2，冷查询）：flowtrace-query 列前缀 + Parquet 谓词下推（ts range）。
- 删除：先在 `deletes/` 记墓碑（查询时过滤），后台 compaction 惰性重写剔除该用户对象；7 天内完成物理删除（合规承诺口径）。

## 10. 数据删除矩阵（`DELETE /v1/users/{userId}/traces`，D14）

| 存储 | 动作 | 时机 |
|---|---|---|
| Redis 墓碑 | `SET tomb:u:{userId}` TTL 24h | 作业开始前（拦住在途 Kafka 数据） |
| `trace_point` / `trace_segment` / `trace_batch` / `dirty_day` | 逐分区 `DELETE WHERE user_id = ?`（每分区独立事务，断点续跑，进度写作业表） | 作业中 |
| Redis 位置 | `DEL rt:pos:u:{userId}`；`rt:pos:d:*` 属于该用户的（由 device 表反查 device_id 集合）逐一 DEL | 作业中 |
| Kafka | 不删；retention 72h 自然过期，在途消息被墓碑拦截 | — |
| Parquet | `deletes/` 墓碑 + 惰性重写（§9） | 7 天内 |
| DB 备份/PITR | **不追溯**（明示限制：备份恢复可能带回已删数据，需在合规文档向用户声明，恢复演练时按墓碑清单二次清理） | — |

完成判定：全部存储分项成功 → 作业状态 DONE → 写 `audit_log(USER_TRACES_DELETE)`；任一失败 → 作业可重跑（幂等：删除是幂等操作）。
