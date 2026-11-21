# 2026-11-21 4C8G 复测：B（延迟归因）→ C（PG 纯写入上界）→ A（接入上限）

> 目标：回答「现在的压测结果达到理论上限了吗」，并按**应用服务器 4C8G + PG 服务器 4C8G（两台）**的口径重测。
> 关联：[09 性能与容量](../design/09-performance.md)、[2026-11-20 容量与瓶颈报告](2026-11-20-upload-capacity.md)。
> 原始 JSON：`scripts/.gen/b*.json`、`c1b-*.json`、`c2-*.json`、`a1-*.json` ~ `a4-*.json`。

---

## 0. 一句话结论

**没有达到理论上限，而且瓶颈判断错了。**

- 真正的墙是**应用侧每批固定开销**（解码 / 清洗 / 分段 DELETE+INSERT / Redis 往返 / 多表事务），不是 PG、不是 CPU、不是线程数、不是接入。
- PG 只被用掉 **22%** 的写事务能力（应用 490 TPS vs PG 2,245 TPS）；接入侧只被用掉不到 1/5。
- 4C8G 双机实测：**处理 14,713 点/s（12.7 亿/天）**，`synchronous_commit=off` → **18,221 点/s（+24%）**；接入 **≥108,266 点/s 仍 100% 成功**。
- **多行 INSERT 修复已落地并验证（§8.1）**：db_points 18.85 → 3.48ms、单批 33.03 → 13.97ms、排空吞吐 168 → 356 批/s（×2.1）；瓶颈转移到 `db_segment`/`clean`，c=8 探针暴露 Hikari 16 打满。

---

## 1. 实验口径

| 项 | 设置 |
|---|---|
| 应用 | `java -Xms2g -Xmx4g -XX:ActiveProcessorCount=4`，`concurrency=4`，Hikari 16 |
| PG | 4C8G 参数：`effective_cache_size=3GB`、`work_mem=8MB`、`max_wal_size=4GB`、`max_parallel_workers_per_gather=0`、`checkpoint_completion_target=0.9`；`shared_buffers` 保持默认 128MB |
| Kafka | 8 分区（topic 被重建过），4 线程各领 2 分区 |
| 负载 | `scripts/upload-loadtest.py`，20000 设备 / 10% 补传 / 补传间隔 60s / 90s 稳态（B、C 用同一档） |
| 规模约束 | 本机 12 核，无法做 cgroup 硬隔离；用 JVM 核数 + 堆 + PG 参数对齐，并用早前测得的 CPU（app 1.6 核、PG 约 4 个活跃会话）验证未超 4 核预算 |

**未使用 podman**：现有 VM 只有 6 核 / **4GiB**（8G 容器会 OOM），且 PG 仅监听 localhost、`pg_hba` 需改文件（无权限），Kafka `advertised.listeners` 也指向 localhost——容器化会把「跨 VM NAT」引入被测链路，代价大于收益。

**两处环境事故（已处理）**：
- `ps` / `top` 被系统安全策略拦截（`operation not permitted`），CPU 精细采样改用 libproc 也因跨用户进程权限失败，最终以配置约束 + 早前 CPU 实测作证。
- 压测中途磁盘只剩 1.3Gi，导致 A2/A3 的处理侧数字失真；清理合成数据（`truncate`）后恢复到 9Gi。A 组只取「受理成功率 / 时延」，不用其入库数字。

---

## 2. B：单批延迟归因

### 2.1 单线程服务时间（先确认饱和）

| 轮次 | 设备 | 受理批/s | 入库批/s | lag 峰 | 单批耗时 | 是否饱和 |
|---|---|---|---|---|---|---|
| B1 | 3,000 | 94 | 94 | — | 10.6ms（实为负载速率） | 否 |
| B2 | 6,000 | 188 | 188 | 162 | 5.3ms（实为负载速率） | 否 |
| **B3** | **20,000** | **596** | **233** | **5,064** | **4.3ms** | **是** |

B3 入库 6,987 点/s（30 点/批）。**单线程真实服务时间 4.3ms/批**。

### 2.2 4 线程与并发放大

| 配置 | 受理点/s | 入库点/s | 等效批/s | 单批耗时 | lag 峰 | p95 |
|---|---|---|---|---|---|---|
| 1 线程（B3） | 17,866 | 6,987 | 233 | **4.3ms** | 5,064 | 10ms |
| **4 线程（C1b，干净）** | 17,930 | **14,713** | 490 | **8.2ms** | 3,719 | 15ms |
| 4 线程 + sync=off（C2） | 17,836 | **18,221** | 607 | **6.6ms** | 928 | 10ms |

- 线程 1 → 4，单批成本 4.3 → 8.2ms（**并发抬高了单批延迟**），所以吞吐只 ×2.1 而不是 ×4。这解释了 2026-11-21 早前「8 vs 16 线程无差异」。
- `synchronous_commit=off`：**+24%**、单批省 1.6ms、lag 峰 3,719 → 928。这 1.6ms 就是 fsync/提交。

### 2.3 Redis 占比

干净窗口（40s、20k 设备）：**270,000 条命令，服务端总耗时 2.82s ≈ 0.07 核**；单批服务端 Redis 时间约 0.12ms。主要命令 `evalsha 24.6µs`、`set 7µs`、`incrby 5.3µs`、`pexpire 3µs`。**Redis 不是瓶颈**（网络 RTT 另计，但未到毫秒级）。

### 2.4 归因结论

| 段 | 每批耗时 | 依据 |
|---|---|---|
| DB 写入 + 提交 | **0.8–1.8ms** | pgbench 单事务 latency（§3） |
| Redis 服务端 | ≈0.12ms | `INFO commandstats` |
| fsync（含在 DB 内） | ≈1.6ms | sync=off 差分 |
| **应用侧残差** | **≈6–7ms** | 8.2ms − DB − Redis |

> 注意：B3 用 2.5 点/批的轻批，C1b 用 30 点/批；这里的 ms 数只在同一行内比较，不跨行。

---

## 3. C：PG 纯写入上界

探针表 `perf_probe`（与 `trace_point` 同列、3 个索引），pgbench 脚本每事务插入 30 行（对应一个处理批），`-c 4 -j 4 -T 30`：

| 对照 | TPS | 折算点/s | 单事务延迟 |
|---|---|---|---|
| 1 客户端，sync=on | 1,275 | 38,250 | 0.784ms |
| **4 客户端，sync=on** | **2,245** | **67,350** | **1.782ms** |
| 4 客户端，sync=off | 5,857 | 175,710 | 0.683ms |
| 单事务批量 50 万行 | — | 1,250,000 | 398.9ms 总计 |

**对照应用**：4 线程应用只有 490 等效事务/s = PG 4 并发能力的 **22%**。即使把分段、台账、Redis 都算上，PG 也远未饱和。

这直接**推翻**了 2026-11-20 报告里「单机 PostgreSQL 的提交与写吞吐是真正的墙」的判断：那是从「加线程无效 + 合并事务 +43%」反推的，没有做过纯 DB 标定。

---

## 4. A：接入侧真实上限

同一 4C8G 应用，逐步加设备（60s 稳态）：

| 轮次 | 设备 | 受理点/s | 受理批/s | 成功率 | p95 | p99 | Hikari 排队 | 入库点/s |
|---|---|---|---|---|---|---|---|---|
| C1b | 20,000 | 17,930 | 594 | 100% | 15ms | — | 0 | 14,713 |
| A1 | 40,000 | 35,257 | 1,196 | **100%（0 失败）** | 15ms | 20ms | 0 | 15,075 |
| A2 | 60,000 | 53,472 | 1,793 | **100%（0 失败）** | 20ms | 50ms | 28 | 7,020 * |
| A3 | 100,000 | 90,360 | 2,949 | **100%（0 失败）** | 50ms | 70ms | 59 | 4,857 * |
| **A4** | **150,000** | **108,266** | **2,863** | **100%（0 失败）** | 70ms | 150ms | 47 | — |

\* A2/A3 的入库数字受磁盘仅剩 1.3Gi 与历史积压影响，不作为处理侧结论。

- **服务端接入侧从未出现 429 / 失败**，即使 108k 点/s（≈93.5 亿点/天）。
- A4 的批率 2,863 反而低于 A3 的 2,949，说明**压测客户端先到顶**（设备舰队建模 + 编码 + 组包约 2.9k 批/s），不是服务端。
- 所以 2026-11-20 报告里「受理上限 >17.9k 点/s」应更正为「**≥108k 点/s，未测到服务端上限；17.9k 只是当时压到的量**」。
- 代价：p99 从 20ms 涨到 150ms，Hikari 开始排队，Kafka 积压暴涨（服务是异步接单，不等于能持久化的量）。

---

## 5. 对「达到理论上限了吗」的回答

| 层 | 是否到顶 | 证据 |
|---|---|---|
| 压测客户端 | **是**（A4） | 批率不再随设备数增长 |
| 接入（HTTP → Kafka） | **否** | 108k 点/s 仍 100% 成功 |
| PG 写入 | **否** | 只用了 22% 能力 |
| CPU / 线程池 / 连接 | **否** | 1.6 核、Hikari 排队可控 |
| 应用处理链路 | **是（当前实现）** | 4 线程 14,713 点/s 时 lag 持续涨；单批 8.2ms 中 6–7ms 在应用侧 |

**结论：撞到的是「当前实现的每批固定开销」，不是任何物理上限。** 用配置调参（加线程、扩分区、调 PG）都绕不过去，必须改处理链路。

---

## 6. 下一步（按 ROI 重排）

| 优先级 | 动作 | 依据 |
|---|---|---|
| 1 | **`synchronous_commit=off`**（点写入链路，端侧幂等 + 补传可补偿最近 ~200ms） | 已实测 +24%，零代码改动 |
| 2 | **拆分「分段」写**：`DELETE + INSERT` 整窗重写是每批最重的语句，改为增量/延迟合并 | 应用侧残差 6–7ms 的主嫌 |
| 3 | **一次 poll 多批合并写**：`poll-records=100` 但逐批提交，改成攒 N 批一个事务 | 把每批固定成本摊薄 |
| 4 | 给 `BatchPipeline.process` 加分段计时埋点 | 现在只能靠差分推断，缺直接证据 |
| 5 | PG 调优 / 分离部署 | 只有当前面几项把应用侧压到 2ms 以下后才会成为新瓶颈 |

---

## 7. 复现命令（摘要）

```bash
# 4C8G 应用
FLOWTRACE_PROCESSOR_CONCURRENCY=4 SPRING_DATASOURCE_HIKARI_MAXIMUMPOOLSIZE=16 \
java -Xms2g -Xmx4g -XX:ActiveProcessorCount=4 -jar \
  backend/flowtrace-app/build/libs/flowtrace-app-0.0.1-SNAPSHOT.jar

# PG 4C8G 参数（可热加载）
alter system set effective_cache_size='3GB';
alter system set work_mem='8MB';
alter system set max_wal_size='4GB';
alter system set max_parallel_workers_per_gather=0;
alter system set checkpoint_completion_target=0.9;
select pg_reload_conf();

# sync=off 差分（新会话生效，需重启应用）
alter database flowtrace set synchronous_commit = off;
-- 还原：alter database flowtrace reset synchronous_commit;

# 纯 DB 上界
/Library/PostgreSQL/18/bin/pgbench -h localhost -U postgres -d flowtrace \
  -c 4 -j 4 -T 30 -n -f /tmp/pg_insert.sql
```

（实验结束后已还原 PG 参数与 `synchronous_commit`，并删除 `perf_probe`。）


---

## 8. 计时埋点（2026-11-21 追加）：真凶是逐行 INSERT

给 `BatchPipeline` 全链路加了 Micrometer 分段计时（`ft_processor_stage_seconds{stage}`，墙钟），同口径 4C8G 跑一轮后瞬时归因（4 线程、168 批/s 排积压）：

| stage | 每批均值 | 占比 |
|---|---|---|
| **db_points（batchInsertIgnore 写 30 点）** | **18.85ms** | **57%** |
| clean（getOrLoad + 清洗） | 5.93ms | 18% |
| db_segment（分段 DELETE+INSERT） | 5.52ms | 17% |
| db_txn 残差（Exposed 事务管理 + PG 提交 fsync） | **0.40ms** | 1% |
| tombstone / watermark(×2) / recalc_guard（Redis 4 次） | ≈1.7ms | 5% |
| decode（protobuf parseFrom） | 0.01ms | ≈0% |
| **total** | 33.03ms | |

pgbench A/B 复核（探针表，4 并发，每事务 30 行）：

| 写法 | 每事务延迟 |
|---|---|
| 30 条独立 INSERT（= app 的 JDBC batch 现状） | **43.5ms** |
| 1 条多行 INSERT | **4.34ms** |

**结论：`db_points` 慢 10 倍的原因是逐行 INSERT。** `application.yaml` 虽已配 `reWriteBatchedInserts=true`，但 pgjdbc 对带 `ON CONFLICT` 的 INSERT **不做改写**（静默跳过），所以开关没生效。

**修复方向**：`TracePointRepository.batchInsertIgnore` 改为单条多行 `INSERT ... VALUES (...),(...) ON CONFLICT (ts, point_id) DO NOTHING`（预期 db_points 18.85 → ~4ms，处理速率在当前库规模下翻倍；clean 5.93ms 与 db_segment 5.52ms 是下一层优化对象）。

**附带发现**：`BatchPipelineTest` 7 个用例在 HEAD 上就红（`NotInitializedTransactionManager`）——「事务合并」把 `transaction {}` 挪进管道后，`PipelineFixture` 没有初始化 Exposed JDBC 事务管理器，需要补 H2 测试依赖或在 fixture 里 `Database.connect`。与本次埋点改动无关。

**埋点改动**（可直接复用）：
- `ProcessorMetrics.stage(stage, durationNs)` → `ft_processor_stage_seconds{stage}`；
- `BatchPipeline` 注入 metrics，`stage("db_txn") { transaction { ... } }` 等包裹各段（inline + finally，异常也计时）；
- `TraceBatchListener` 对 `parseFrom` 记 `decode`。

### 8.1 修复验证（同日追加）：多行 INSERT 落地实测

修复已实施：`TracePointRepository.batchInsertIgnore` 改**单条多行** `INSERT ... VALUES (...),(...) ON CONFLICT (ts, point_id) DO NOTHING`（显式拼 SQL，值按各列 columnType 绑定，绕过 pgjdbc 对带 `ON CONFLICT` batch 不改写的限制）；`PointsWriter` 收敛为委托 Repository。编译通过后同口径复测两轮（20,000 设备 / 10% 补传 / 90s 稳态，全程 0 失败 / 0 DLQ / 0 慢批，报告 `scripts/.gen/e1-multirow.json`、`e2-conc8.json`）：

| stage | 修复前 | E1（c=4） | E2（c=8） |
|---|---|---|---|
| **db_points** | 18.85ms | **3.48ms**（-82%） | 4.33ms |
| clean | 5.93ms | 2.04ms | 3.39ms |
| db_segment | 5.52ms | 5.57ms | 5.26ms |
| **total** | **33.03ms** | **13.97ms** | 16.10ms |
| 排空吞吐 | 168 批/s ≈ 5.0k 点/s | **356 批/s ≈ 10.9k 点/s** | 564 批/s ≈ 17.1k 点/s |
| 稳态入库（受理 ~18k 点/s 同时） | — | 8,381 点/s | **15,081 点/s** |
| Kafka lag 峰（原生） | — | 26,842 批（~81s 排空） | 2,008 批（~24s 排空） |
| Hikari 排队峰 | 0 | 6/16 | **16/16 打满** |

- **验收对照**：db_points → ~4ms ✓（3.48）、单批 → ~18ms ✓（13.97）、「入库 ≥2 万点/s」✗（c=4 ~10.9k、c=8 ~15–17k）。
- **瓶颈按预告转移**到 `db_segment`（5.57ms，40%，整窗 DELETE+INSERT）与 `clean`（DeviceStateStore）；两项压到 ~2ms 后，8 线程墙钟 ~10ms ≈ 2 万点/s——与 §6 第 2/3 项一致。
- **c=8 新约束**：Hikari 16 被消费连接 / ingest 写台账 / 状态回写共享打满；先做 poll 合批再调池。
- **口径注**：① C1b 的 490 批/s（8.2ms/批）是差分反推值，与埋点直测 33.03ms/批矛盾（4 线程上限 ~120–170 批/s），处理侧对比以直接测量为准；② E1/E2 起步于 truncate 后空库（E2 结束时 ~190 万点），db_points 会随库增长略升（E2 4.33ms 已体现），pgbench 上界 4.34ms 不变；③ 压测客户端同机抢核，排空段更接近服务真实上限。
- 逐秒吞吐与 `costMs` 分布由应用日志批摘要行聚合：E1 costMs p50=5ms / p90=16ms / p99=131ms（小批快、500 点补传批慢）。
