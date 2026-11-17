# 05 SDK 详细设计（react-native-flowtrace）

> 上游：[01-总体设计](01-overview.md)（决策 D10/D11，风险 R1/R8）、[02-协议与编码](02-protocol.md)｜ 落地位置：`sdk/react-native-flowtrace/`

## 1. 工程形态

标准 React Native 库工程：`create-react-native-library`（react-native-builder-bob + codegen），**turbo-module 官方组合：Android 用 Kotlin（codegen 包名 `dev.yidafu.flowtrace`）、iOS 用 Objective-C**（AGENTS.md 约定；Swift/nitro 路线已评估并放弃）。

```text
sdk/react-native-flowtrace/
├── package.json              # name: react-native-flowtrace
├── Flowtrace.podspec
├── tsconfig.json
├── src/                      # TS 源码（构建产物 lib/ 不入库）
│   ├── index.ts              # 唯一对外出口（createFlowTrace 及类型）
│   ├── core/
│   │   ├── Sampler.ts        # 动态采样决策（状态机）
│   │   ├── UploadScheduler.ts# 双通道调度/退避
│   │   └── types.ts          # 配置/事件类型（含默认值）
│   ├── storage/
│   │   ├── Queue.ts          # 本地队列门面（调原生模块）
│   │   └── InMemoryQueue.ts  # 测试后端（同一接口）
│   ├── transport/
│   │   ├── Encoder.ts        # Protobuf + Delta + GZIP（唯一编码实现，D11）
│   │   ├── Uploader.ts       # HTTP 上传（实时通道）
│   │   └── proto/            # @bufbuild/protobuf 生成代码（提交入库）
│   ├── location/
│   │   ├── LocationProvider.ts   # 定位/运动/围栏抽象接口
│   │   └── NativeLocationProvider.ts
│   └── ui/                    # 轨迹管理页面（可选子入口 "./ui"，见 §9）
│       ├── FlowTraceManagerScreen.tsx   # 页面骨架（概览/列表/地图三 Tab）
│       ├── PointListScreen.tsx          # 点列表（状态过滤/分页）
│       ├── PointDetailSheet.tsx         # 点详情弹层
│       └── TraceMapView.tsx             # MapLibre 地图
├── android/                  # Kotlin 原生模块（codegen spec 对应实现）
│   └── .../flowtrace/        # LocationModule / QueueModule(Room) / SchedulerModule(WorkManager+前台服务)
├── ios/                      # Objective-C 原生模块（turbo-module，专项设计见 §10）
│   ├── Flowtrace.h/.mm       # Spec 入口（getTurboModule）
│   ├── FTLocationModule.mm   # CoreLocation + CoreMotion + CLCircularRegion（§10.2~§10.4）
│   ├── FTQueueModule.mm      # sqlite3 C API 薄封装（§10.5）
│   └── FTSchedulerModule.mm  # BGTaskScheduler + 后台 URLSession（§10.6）
└── src/__tests__/            # golden vectors + 采样/调度单测
```

依赖：TS 运行时 `@bufbuild/protobuf` + `pako`；Android `play-services-location`、`androidx.work:work-runtime`、`room`；iOS 系统 SQLite3（C API 薄封装，无第三方库）+ `CoreLocation`/`CoreMotion`/`BackgroundTasks`/`Network`（NWPathMonitor）/`UIKit`——全部系统框架（链接清单 §10.8）。**原生侧不实现编码**（D11），只做采集、存储、调度、围栏。UI 子入口 `./ui` 的 MapLibre（`@maplibre/maplibre-react-native`）为 **peerDependency**，仅宿主集成轨迹管理页面时安装（§9.1 构建隔离）。

## 2. 公开 API

```ts
export interface FlowTraceConfig {
  endpoint: string                       // https://…/v1
  tokenProvider: () => string | Promise<string>   // JWT 获取/刷新（401 时重新调用）
  deviceId: string                       // 安装级稳定 ID（首启生成并持久化）
  sampling?: Partial<SamplingConfig>
  realtime?: Partial<RealtimeConfig>
  backfill?: Partial<BackfillConfig>
  queue?: Partial<QueueConfig>
  purgeOnRevoke?: boolean                // 默认 true：撤回授权即清未上报数据
}

export interface FlowTrace {
  start(opts?: { sessionId?: string; taskId?: string }): Promise<void>
  stop(): Promise<void>                  // 停采集与上报，队列保留
  addGeofence(g: { id: string; lat: number; lng: number; radiusM: number }): Promise<void>
  removeGeofence(id: string): Promise<void>
  flush(channel?: 'realtime' | 'backfill', opts?: { force?: boolean }): Promise<void>
    // 立即组批上传一轮。force=true：补传通道跳过 WiFi/充电与积压准入（单批上限不变），
    // 并唤醒退避静默（ABANDONED）中的批、复位 attempt —— 管理页面「立即补传」按钮（§9.2）
  purgePending(): Promise<void>          // 隐私：清空未上报数据（INBOX/PENDING/INFLIGHT）
  purgeAll(): Promise<void>              // 清空本地全部数据（含 SENT 历史与 batch 台账）
  getDiagnostics(): Promise<Diagnostics>
  on(event: 'status' | 'error', listener: (e: FlowTraceEvent) => void): () => void
  readonly inspect: TraceInspector       // 只读查询（管理页面/宿主自定义 UI 用，§2.1）
}
```

`FlowTraceEvent`：`{ type: 'queue-depth', depth, oldestPendingMs } | { type: 'upload', channel, ok, points, attempts, nextIntervalSec? } | { type: 'mode', mode, reason } | { type: 'geofence', id, event: 'enter'|'exit', tsMs } | { type: 'error', code, message, fatal? }`。

### 2.1 本地数据查询（TraceInspector，轨迹管理页面数据源）

```ts
export type LocalPointStatus = 'inbox' | 'pending' | 'inflight' | 'sent'
export interface LocalPointRow {
  id: string; sessionId?: string; taskId?: string
  tsMs: number; latE7: number; lngE7: number          // 量化整数，展示层 /1e7
  accuracyCm?: number; speedMps?: number; bearingDeg?: number; altitudeCm?: number
  source: number; flags: number
  status: LocalPointStatus
  batchId?: string; batchIdx?: number; sentAtMs?: number
}
export interface LocalBatchRow {
  batchId: string; channel: 'realtime' | 'backfill'
  status: 'open' | 'succeeded' | 'abandoned' | 'discarded'
  pointCount: number; attempts: number; lastError?: string   // "HTTP_429" / "NETWORK" / "HTTP_422"…
  createdAtMs: number; updatedAtMs: number
}
export interface QueueStats {
  inbox: number; pending: number; inflight: number; sent: number   // sent = 保留窗口内计数
  oldestPendingMs?: number; dbBytes: number
  mode: string; nextIntervalSec?: number
  lastSuccessAtMs?: number; lastError?: string
}
export interface TraceInspector {
  queryPoints(f?: { status?: LocalPointStatus[]; fromTsMs?: number; toTsMs?: number;
                    limit?: number; cursor?: string }): Promise<{ rows: LocalPointRow[]; nextCursor?: string }>
  getPoint(id: string): Promise<LocalPointRow | null>
  getBatch(batchId: string): Promise<LocalBatchRow | null>
  getQueueStats(): Promise<QueueStats>
}
```

查询走原生（Room / sqlite3，索引 `idx_point_status_ts`），`limit` 默认 200、上限 1000；**只读**，永不修改队列状态。

配置默认值（与 01/04 文档参数一致）：

```ts
const DEFAULT_SAMPLING: SamplingConfig = {
  modes: {
    STILL:   { intervalMs: 300_000, minDistM: 0   },
    WALK:    { intervalMs:  20_000, minDistM: 20  },
    RUN:     { intervalMs:  10_000, minDistM: 15  },
    BICYCLE: { intervalMs:  10_000, minDistM: 10  },
    VEHICLE: { intervalMs:   5_000, minDistM: 30  },   // 服务端可调 5~10s（nextUploadIntervalSec 间接调节）
  },
  lowBattery: { thresholdPct: 20, multiplier: 3, maxIntervalMs: 60_000 },
  accuracyDropM: 100,        // 精度劣于此值丢弃（例外：超过 10min 无保留点则收纳）
  motionConfirmCount: 2,     // 运动状态切换需连续 N 次原生信号确认（防抖）
  geofenceBoostRadiusM: 300, // 进入该半径提前提频（驾车档）
}
const DEFAULT_REALTIME: RealtimeConfig = {
  minIntervalSec: 30,        // 初始值；被服务端 nextUploadIntervalSec 覆盖，clamp [10,300]
  triggerPoints: 20,
  maxBatchPoints: 50,
  requestTimeoutMs: 10_000,
  backoffMs: [5_000, 30_000, 120_000, 600_000, 1_800_000],   // 指数退避 + ±20% 抖动
}
const DEFAULT_BACKFILL: BackfillConfig = {
  requireWifi: true, requireCharging: true,   // 与 AGENTS「WiFi/充电」一致，二者同时满足
  minBacklogPoints: 200, minBacklogAgeMs: 600_000,
  maxBatchPoints: 500, maxBatchBytes: 262_144,
}
const DEFAULT_QUEUE: QueueConfig = {
  maxAgeDays: 7, maxPoints: 100_000,     // 未上报行：7 天 / 10 万点 FIFO
  sentKeepHours: 72, sentMaxRows: 20_000 // SENT 历史行：72h / 2 万行（供管理页面查看，§4.2）
}
```

## 3. 分层与 TS/原生职责矩阵（决策 D10）

| 能力 | TS | Android 原生 | iOS 原生 |
|---|---|---|---|
| 定位流 | 消费事件 | FusedLocationProviderCallback（按当前模式的 interval/优先级） | CLLocationManager（desiredAccuracy/distanceFilter 随模式，§10.3） |
| 运动状态 | 消费、确认（迟滞） | ActivityRecognition API | CMMotionActivityManager |
| 地理围栏 | 注册/事件消费、提频决策 | GeofencingClient | CLCircularRegion |
| 采集落库（**JS 挂起也不丢**） | — | 收到修复**直接写 Room**（status=INBOX） | 直接写 sqlite3（status=INBOX） |
| 采样判定 | Sampler 扫描 INBOX → KEEP/DROP | — | — |
| 组批/编码/实时上报 | Scheduler + Encoder + fetch | — | — |
| 后台调度 | 被唤醒执行补传 | 前台服务（location 类型）+ WorkManager 兜底 | BGTaskScheduler + 后台 URLSession 限用 |
| 队列存储 | Queue 门面 | Room（§4.1 schema） | sqlite3（同 schema） |

关键机制——**原生采集 inbox**：原生定位回调把修复直接写入队列表（`status=INBOX`），并节流（≤1 次/秒）通知 JS。Sampler 被唤醒后按 §3.2 规则处理 INBOX 行：保留 → `status=PENDING`；丢弃 → 删行。JS 进程被杀/挂起时原生按**最后同步的模式**继续采集入库；JS 恢复后对积压 INBOX **按同一规则回放判定**（以 `lastKept` 虚拟游标顺序扫描），采样语义与在线时一致。模式切换由 TS 调 `setMode(mode)` 同步原生（挂起期间原生保持旧模式，属可接受降质）。

### 3.2 Sampler 判定规则（core/Sampler.ts）

对每个 INBOX 点 `p`（按 ts 升序，维护 `lastKept`）：

```text
if p.accuracy > accuracyDropM and (lastKept 距 p < 10min):  DROP            # 精度差且不缺点
if p.flags 有 FORCED/ENTER_RADIUS/EXIT_RADIUS:              KEEP, flags 透传
mode = 确认后的当前运动模式（连续 motionConfirmCount 次一致才切换）
prof = modes[mode]（低电量: interval = min(interval × multiplier, maxIntervalMs)）
if lastKept == null:                                        KEEP
if p.ts - lastKept.ts >= prof.intervalMs
   and distM(p, lastKept) >= prof.minDistM:                 KEEP
else:                                                       DROP
KEEP 后: lastKept = p; 若本次为模式切换后首点 → flags |= MOTION_CHANGE
```

- `distM` 用 haversine（本地实现，与后端 GeoMath 同公式）。
- 围栏事件（enter/exit）：原生事件直达 TS，**对应时刻最近的 INBOX 点强制 KEEP 并打 flag**（无 INBOX 点则触发一次即时原生单次定位请求 `getCurrentLocation` 补点）；enter 同时触发实时上报（关键点优先）与提频（`geofenceBoostRadiusM` 内切 VEHICLE 档、退出恢复）。

## 4. 本地队列（storage/Queue）

### 4.1 Schema（Room（Android）/ sqlite3（iOS，ObjC 直封）同构，列名即 DDL）

```sql
CREATE TABLE point (
  id          TEXT PRIMARY KEY,      -- 本地 UUIDv7（仅行标识，不参与服务端身份）
  session_id  TEXT, task_id TEXT,
  ts          INTEGER NOT NULL,      -- epoch ms
  lat_q       INTEGER NOT NULL, lng_q INTEGER NOT NULL,
  accuracy_cm INTEGER, speed_mps REAL, bearing_deg INTEGER, altitude_cm INTEGER,
  source      INTEGER NOT NULL DEFAULT 3,   -- LocationSource
  flags       INTEGER NOT NULL DEFAULT 0,
  status      INTEGER NOT NULL DEFAULT 0,   -- 0 INBOX / 1 PENDING / 2 INFLIGHT / 3 SENT
  batch_id    TEXT, batch_idx INTEGER,
  sent_at     INTEGER,                      -- status=SENT 时写（上报成功时刻）
  created_at  INTEGER NOT NULL
);
CREATE INDEX idx_point_status_ts ON point(status, ts);
CREATE INDEX idx_point_batch     ON point(batch_id);
CREATE INDEX idx_point_sent_at   ON point(sent_at) WHERE status = 3;   -- SENT 淘汰扫描

-- 本地批台账（§4.3 维护；轨迹管理页面「补报诊断」数据源）
CREATE TABLE batch (
  batch_id    TEXT PRIMARY KEY,
  channel     INTEGER NOT NULL,             -- 1 realtime / 2 backfill
  status      INTEGER NOT NULL,             -- 1 OPEN(组批/重试中) / 2 SUCCEEDED / 3 ABANDONED(退避耗尽静默) / 4 DISCARDED(422 毒丸)
  point_count INTEGER NOT NULL,
  attempts    INTEGER NOT NULL DEFAULT 0,
  last_error  TEXT,
  created_at  INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE INDEX idx_batch_status ON batch(status, updated_at);

CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
-- meta keys: lastSuccessTsMs, nextIntervalSec, motionMode, sessionId, schemaVersion, bootedAtMs
```

### 4.2 FIFO 淘汰

淘汰跑在原生侧（批处理事务），避免 JS 卡顿。三类行各自独立：

- **未上报行（INBOX/PENDING）**：`DELETE WHERE created_at < now − maxAgeDays×86400e3` → 若 `COUNT > maxPoints` 按最老超额删除（`status=INFLIGHT` 的行**豁免**，防止删掉传输中的批成员破坏批不变式）。
- **已上报行（SENT）**：更短的独立窗口——`sent_at < now − sentKeepHours×3600e3` 删除；SENT 行数超 `sentMaxRows` 按最老超额删除。**成功不立即删行**是为了轨迹管理页面能展示「是否已上报」（§9）；SENT 行不参与上报路径（组批只取 `status=1`），无性能影响。
- **batch 台账行**：`updated_at < now − 7×86400e3` 删除。

### 4.3 批分配不变式（幂等根基，02 §7）

```text
组批（事务）:
  rows = SELECT * FROM point WHERE status=1 ORDER BY ts ASC LIMIT N      -- N=通道批上限
  对 rows 按 ts 顺序赋 batch_idx = 0..n-1，batch_id = 新 UUIDv7（若行已带 batch_id 则复用：重试场景）
  UPDATE rows SET status=2
  UPSERT batch(batch_id, channel, status=1 OPEN, point_count=n)          -- 本地批台账
编码: Encoder(rows 按 batch_idx 排序)      # 重试输出与首次完全一致的字节
成功: UPDATE rows SET status=3 SENT, sent_at=now；batch.status=SUCCEEDED
失败: UPDATE rows SET status=1（batch_id/idx 保留）；batch.attempts+=1、last_error，回 OPEN 待退避重试
退避耗尽: batch.status=ABANDONED（点行保持 PENDING，等网络变化/前台/手动 flush 唤醒后回 OPEN）
确定性失败(422): 点行删除；batch.status=DISCARDED、last_error=HTTP_422（不再重试）
崩溃恢复（启动时）: UPDATE point SET status=1 WHERE status=2（batch_id 保留）；batch OPEN 行同步复位
```

该不变式保证：同一批字节重试可重放（服务端幂等零副作用）；崩溃后重组批沿用原 batch_id（pointId 派生稳定）。仅当本地库整体丢失（卸载重装/清数据）才可能产生跨 ID 重复，由服务端清洗规则吸收（01 §9 R8）。SENT 行仅作展示保留，组批只取 `status=1`，不影响不变式与上报行为。

## 5. 双通道调度（core/UploadScheduler）

### 5.1 实时通道

触发条件（任一，去抖 1s）：

| # | 条件 | 说明 |
|---|---|---|
| T1 | `now − lastSuccess ≥ 当前 intervalSec` 且有 PENDING 新点 | intervalSec 初值 30，被 200 响应的 `nextUploadIntervalSec` 覆盖（clamp [10,300]） |
| T2 | 新 PENDING 点数 ≥ `triggerPoints`（20） | |
| T3 | 模式切换（确认后） | 该点已带 MOTION_CHANGE |
| T4 | 围栏 enter 事件 | 立即（关键点优先） |

执行：取最老 ≤50 点组批 → 编码 → `POST`（timeout 10s）→ 成功将批内点标 `SENT`（§4.3）、更新 `lastSuccess`/`intervalSec`、事件上报；失败按 `backoffMs[attempt] × (1±0.2 jitter)` 退避，**attempt 计数与补传通道相互独立**（实时失败不阻塞补传，反之亦然）。退避耗尽（5 次）批置 `ABANDONED` 静默：等待网络变化/前台切换/手动 `flush()` 唤醒并复位 attempt。

### 5.2 补传通道

准入：`requireWifi && requireCharging`（原生广播同步给 TS）且（`backlog ≥ minBacklogPoints` 或 `最老 PENDING ≥ minBacklogAgeMs`）。循环：每次取最老 ≤500 点 / ≤256KB（编码后断言）组批上传，串行直至清空或被实时通道触发抢占（补传批完成当前批后让路，两通道对队列的取数区间天然不重叠——实时通道只取它自己组的新批，补传从最老开始）。

手动补报：`flush('backfill', { force: true })` 跳过 WiFi/充电与积压准入（用户明确要求时立即补传），保持单批上限与串行节奏；网络不可用时照常失败进退避。管理页面「立即补传」即走此路径（§9.2）。

### 5.3 后台执行矩阵（风险 R1）

| 能力 | Android | iOS |
|---|---|---|
| 持续采集 | 前台服务（`foregroundServiceType=location`，常驻通知） | `UIBackgroundModes=location`（需 Always 授权；WhenInUse 时后台采集受限，SDK 发 `error(fatal=false)` 提示；授权流程 §10.2、定位映射 §10.3） |
| 实时上报 | 前台服务进程内 JS 定时器持续有效 | 前台有效；进入后台用一次 ~30s 的后台任务尽量完成在途批，未完成按失败退避（§10.6） |
| 补传 | 路径 A：前台服务存活时 JS 循环直接跑；路径 B：WorkManager（约束 WiFi+充电，周期 ≥15min）拉起 headless JS 执行一轮补传循环 | 前台/后台短窗口每轮 ≤2 批；`BGProcessing`（WiFi+充电约束）+ `BGAppRefresh` 机会性补跑；**残余积压待下次前台会话清空**（产品约束，文档明示；§10.6） |
| 围栏 | GeofencingClient（系统级，进程死后可唤醒） | CLCircularRegion（同；上限 20 个，§10.4） |

## 6. Uploader（transport/Uploader.ts）

- `fetch` + `Authorization: Bearer(await tokenProvider())`；401 → 重新取 token 重试一次；403(`DEVICE_DISABLED`) → 停通道、事件 `error(fatal=true)`；429 → 按 `Retry-After` 挂起实时通道；422 → 整批放弃（点行删除、batch 标 `DISCARDED` + 指标，02 §9 语义，管理页面标红展示）；网络异常/5xx → 退避。
- 每次结果发 `upload` 事件并累计到 `Diagnostics`（成功率、attempt 分布、字节数）——用于运营对账「上报成功率 >99%」。

## 7. 隐私与权限

| 场景 | 行为 |
|---|---|
| 初始化 | `start()` 检查定位授权；iOS 后台采集需 `Always`（`WhenInUse` 时正常前台采集，后台受限并明确事件告知；授权流程与降级见 §10.2） |
| 撤回授权（系统事件） | 立即 `stop()`；`purgeOnRevoke=true` 时调 `purgePending()`（删队列表全部未上报行），已上报数据由服务端 `DELETE /v1/users/{id}/traces` 负责（宿主 App 触发） |
| 本地 SENT 历史 | 已上报数据的本地副本（72h/2 万行窗口自动淘汰，§4.2）；`purgeAll()` 立即清除（管理页面「清空本地数据」）；宿主调用服务端删除 API 后可自行决定是否 `purgeAll()` |
| 业务删除 | 宿主 App 调服务端删除 API；SDK 无需参与 |
| 诊断数据 | `getDiagnostics()` 只含计数与状态，不含坐标 |

## 8. 测试策略

| 层 | 内容 |
|---|---|
| Encoder | `proto/testdata/v1` golden vectors 全量断言（字节 sha256 + 解码语义 + pointId）；量化/排序/尺寸断言单测 |
| Sampler | 表驱动用例：各模式 interval/dist、迟滞确认、低电量、精度丢弃例外、围栏强制、崩溃回放（INBOX 积压一次性回放结果 = 在线逐点一致） |
| Queue | InMemoryQueue 与原生队列同接口测试套件（FIFO 淘汰、批不变式、崩溃恢复） |
| Scheduler | 虚假时钟 + FakeLocationProvider：T1~T4 触发矩阵、退避序列与抖动边界、双通道互不阻塞 |
| Uploader | fetch mock：401 刷新、429 挂起、422 毒丸放弃、断网恢复 |
| Inspector | `queryPoints` 过滤/游标分页正确性；`getQueueStats` 与实际行数一致；SENT 淘汰窗口边界（72h/2 万行） |
| 管理页面 | 组件冒烟（examples App）：三 Tab 渲染、状态过滤联动、「立即补传」走 `flush('backfill',{force})` 路径、地图点着色/聚合/详情跳转正确 |
| 集成 | examples/ RN 示例 App 手工回归（Android 真机后台杀进程 24h、iOS 后台 8h） |

## 9. 轨迹管理页面（FlowTraceManagerScreen，可选 UI 子入口）

面向巡检员/运维的本地轨迹管理页面：查看本地点及其**上报状态**、手动补报、点详情、MapLibre 地图展示。**纯本地**——只读本地队列 + 触发既有动作 API，不发网络请求、不引入任何服务端改动。

### 9.1 构建隔离与集成方式

- 独立子入口：`import { FlowTraceManagerScreen } from 'react-native-flowtrace/ui'`（package.json `exports` 分离），主入口零 UI 依赖；`@maplibre/maplibre-react-native` 为 peerDependency，宿主集成 UI 时自行安装。
- 定位为调试/运维工具：建议宿主仅在 debug 构建、或生产 feature flag + 门禁（密码/生物识别，宿主实现）下挂载（如 `navigation.navigate('FlowTraceManager')`）。
- Props：

```ts
export interface FlowTraceManagerScreenProps {
  mapStyle?: string | object      // MapLibre style URL/JSON；默认 OSM raster demo 样式
  defaultRangeMs?: number         // 列表/地图默认时间窗，默认 24h
  maxMapPoints?: number           // 地图单窗口点数上限，默认 5000（超出提示收窄时间窗）
  header?: React.ReactNode        // 宿主自定义头部（标题/返回按钮）
}
```

### 9.2 页面结构（三 Tab）

**概览**（数据：`inspect.getQueueStats()` + 事件流实时刷新）

- 统计卡：INBOX / PENDING / INFLIGHT / SENT（窗口内）计数、最老积压时长、库体积、当前采样模式、`nextIntervalSec`、最近上报结果（成功时间/错误摘要）。
- 动作按钮：
  - 「立即上报」→ `flush('realtime')`
  - 「立即补传」→ `flush('backfill', { force: true })`（跳过 WiFi/充电准入；进行中显示进度 = 已完成批数 / 剩余 PENDING 点数）
  - 「清空本地数据」→ `purgeAll()`，二次确认（不可恢复）
- 失败批摘要（`batch.status = ABANDONED / DISCARDED`）：批数、最近 `last_error`；点击跳列表按状态过滤。

**列表**（数据：`inspect.queryPoints()`，FlatList + 游标分页，`limit=200`）

- 过滤：状态 chips（全部 / 未上报=INBOX+PENDING / 上报中=INFLIGHT / 已上报=SENT）+ 时间范围（1h / 24h / 7d / 自定义，与地图共享）。
- 行摘要：时间 `HH:mm:ss`、坐标（度，6 位小数）、状态徽标、来源/精度/速度；`flags` 非零时角标图标（围栏进/出、运动切换）。
- 点击行 → **详情弹层**（PointDetailSheet）：全字段表——本地 id、ts（毫秒+本地时区）、量化坐标与度坐标、accuracy/speed/bearing/altitude、source、`flags` 逐位解析徽标、sessionId/taskId、`batchId` + `batchIdx`、`sentAtMs`；所属批信息（`inspect.getBatch()`）：通道、`attempts`、`last_error`、批状态（SUCCEEDED/ABANDONED/DISCARDED 着色）；「在地图上查看」→ 切地图 Tab 并 `flyTo`。

**地图**（TraceMapView，MapLibre）

- 数据：当前过滤条件下的点 → 内存构建 GeoJSON `FeatureCollection` → `ShapeSource`；`CircleLayer` + 聚合（点数 > 1000 时 `cluster: true`、clusterRadius 40px，点击聚合数字展开）。
- 状态配色（图例常驻）：INBOX `#9E9E9E` 灰 / PENDING `#F57C00` 橙 / INFLIGHT `#1976D2` 蓝 / SENT `#388E3C` 绿。
- 可选轨迹连线：按 `sessionId` 分组、时间序连接窗口内 SENT+PENDING 点的 `LineLayer`（开关，默认关）。
- 交互：点击 marker → 详情弹层；与列表共享过滤状态。
- 性能：单窗口 ≤ `maxMapPoints`（默认 5000），超出截断并提示收窄时间范围。

### 9.3 数据流与性能

- UI **只经** `ft.inspect`（只读）与 `flush/purgeAll`（动作），不触碰队列内部状态机；查询走原生索引 `idx_point_status_ts`（200 行 < 10ms），结果一次序列化过 bridge。
- 概览卡订阅 `queue-depth` / `upload` / `mode` / `error` 事件实时更新，无需轮询；列表与地图共享同一查询结果缓存（UI 层，TTL 5s + 下拉强制刷新）。
- 页面卸载即停查询；不持有任何写锁，不影响上报链路。

### 9.4 隐私与安全

- 页面所有坐标均来自本地队列；不新增采集、不上传任何页面数据。
- 生产集成建议：feature flag 控制入口 + 宿主门禁；「清空本地数据」双确认；详情支持一键复制量化坐标（调试排障）。
- 页面不请求定位授权（采集授权仍由 `start()` 流程管理）。

## 10. iOS 平台专项设计

> 汇总 iOS 侧落地细节，与 §3 职责矩阵、§5.3 后台执行矩阵、§7 隐私权限一一对应。D10（策略在 TS、原生薄）/ D11（编码只在 TS）原则不变；原生全部 Objective-C + 系统框架，零第三方 pod（Swift/nitro 路线已评估放弃，AGENTS.md 约定）。

### 10.1 原生模块结构（与 Android 一一对应）

| ObjC 类 | 对应 Android | 职责 |
|---|---|---|
| `Flowtrace`（Spec 入口） | `FlowtraceModule` | codegen Spec 方法实现 + `getTurboModule`；§2/§2.1 的原生原子操作统一在此注册转发：采集启停、`setMode`、围栏注册、队列读写/淘汰/批收尾、Inspector 查询、后台任务注册 |
| `FTLocationModule` | FusedLocation + ActivityRecognition + GeofencingClient | `CLLocationManager` 生命周期与随模式参数切换（§10.3）、`CMMotionActivityManager`、`CLCircularRegion`、采集 inbox 直接落库（§3 机制） |
| `FTQueueModule` | Room DAO | sqlite3 C API 薄封装（§10.5）：查询、批分配、FIFO 淘汰、崩溃恢复 |
| `FTSchedulerModule` | WorkManager + 前台服务 | `BGTaskScheduler` 注册/提交、后台窗口管理、后台 URLSession 补传（§10.6） |

事件回 JS：经 Spec 声明的 emitter（codegen 生成）推采集入库通知（节流 ≤1 次/秒，§3）、`geofence`、`mode`、`error` 等；JS 挂起期间通知丢弃——数据已落库，恢复后由 INBOX 回放补齐（D10，无丢失语义）。

### 10.2 权限、授权与系统能力降级

宿主 `Info.plist` 清单（SDK README 提供集成说明）：

| Key | 用途 |
|---|---|
| `NSLocationWhenInUseUsageDescription` | 前台采集必需 |
| `NSLocationAlwaysAndWhenInUseUsageDescription` | 后台连续采集 + 围栏 |
| `NSMotionUsageDescription` | `CMMotionActivityManager` 运动识别（被拒时降级为速度推断，§10.3） |
| `UIBackgroundModes` | `location` / `fetch` / `processing` |
| `BGTaskSchedulerPermittedIdentifiers` | `dev.yidafu.flowtrace.refresh`、`dev.yidafu.flowtrace.backfill`（§10.6） |

授权流程（`start()` 内）与降级规则：

- **两段式 Always**：先 `requestWhenInUseAuthorization` 保证前台采集可用；后台采集再 `requestAlwaysAuthorization`。iOS 常以「暂定 Always」授予（行为等同 WhenInUse，系统稍后引导用户确认升级）——SDK 以 `authorizationStatus` 为准动态切换，不假设一次到位。
- `allowsBackgroundLocationUpdates` **仅在 Always + `UIBackgroundModes: location` 齐备时置 YES**，否则置 NO（条件不满足时置 YES 会直接 crash）。
- **围栏仅 Always 可用**：未授权时 `addGeofence` 返回 `GEOFENCE_UNAVAILABLE`；WhenInUse → Always 升级后自动补注册。
- **「精确位置」开关关闭**（系统 reduced accuracy）：回调精度劣化至公里级，持续命中 `accuracyDropM` 丢弃规则（仅 10min 兜底例外留极稀疏点）；发一次性 `error(fatal=false)` 事件提示宿主引导开启。
- **低功耗模式**（`isLowPowerModeEnabled`）：等同 `lowBattery` 分支（间隔 ×3），与电池电量阈值取更严者。
- 授权变更监听（`didChangeAuthorizationStatus`）：被拒/受限 → 立即 `stop()` + `purgePending()`（§7 撤回授权语义）；升级 Always → 自动启用后台采集与围栏。

### 10.3 定位与运动映射（CLLocationManager 随模式切换）

iOS 无「每 N 秒回调」语义：采样 interval 映射为 `distanceFilter` + `pausesLocationUpdatesAutomatically`，`activityType` 供系统省电优化：

| 模式 | desiredAccuracy | distanceFilter | activityType | 备注 |
|---|---|---|---|---|
| STILL | HundredMeters | — | Other | 停用连续定位，改 `startMonitoringSignificantLocationChanges`（百米级、极省电；回调后临时起一次精确定位补点） |
| WALK | NearestTenMeters | 20m | Fitness | |
| RUN | NearestTenMeters | 15m | Fitness | |
| BICYCLE | NearestTenMeters | 10m | Fitness | |
| VEHICLE | Best | 30m | AutomotiveNavigation | |

- `showsBackgroundLocationIndicator = YES`（状态栏蓝胶囊，合规要求）；STILL ↔ 其他模式互切即 SLC/连续定位切换，由 TS `setMode()` 驱动（§3：挂起期间保持旧模式）。
- **运动识别**：`CMMotionActivityManager.startActivityUpdates` 过渡事件（walking/running/cycling/automotive/static）作为 §3.2 `motionConfirmCount` 确认源，**confidence < medium 的过渡不计数**（防误抖）。权限被拒时降级：按 `speedMps` 分档推断模式（阈值常量与 Android ActivityRecognition 过渡语义对齐），并发一次性 `error` 事件告知。

### 10.4 地理围栏（CLCircularRegion）

- **上限 20 个/App**（Android GeofencingClient 为 100）：`addGeofence` 超限返回 `GEOFENCE_LIMIT_EXCEEDED`（与 Android 超限行为对齐），P 阶段以「巡检点 ≤20」为产品约束明示，不做静默淘汰。
- 半径经验上 ≥100m 才可靠（系统不承诺过小半径的触发）；`notifyOnEntry/Exit` 双向注册，事件语义与 §3.2 / §5.1-T4 不变。
- 进程被杀/系统重启后区域监控由系统恢复；围栏触发可**拉起被杀的 App**（launchOptions 冷启动交付）——`FTLocationModule` 初始化时读取补处理，走「INBOX 回放 + 实时上报」标准路径。

### 10.5 本地队列（sqlite3 C API 要点）

- **单连接 + 专用串行 dispatch queue**：所有 SQL（含查询）入队执行；打开即 `journal_mode=WAL`、`busy_timeout=3000`。读多写少场景 WAL 足够，不做读写分连接。
- Schema 与 §4.1 完全同构（partial index `WHERE status=3` 为 SQLite 原生能力）；迁移按 `meta.schemaVersion` 顺序执行编号迁移函数，与 Android Room migration 一一对应。
- 事务边界与 §4.3 一致：组批/淘汰/崩溃恢复统一 `BEGIN IMMEDIATE`（写事务串行化，天然防并发插队）；prepared statements 初始化时预编译复用。
- 崩溃恢复 = WAL 自愈 + §4.3 `status=2` 复位语句启动时执行一次，无平台差异。

### 10.6 后台调度与补传（BGTask + 后台 URLSession）

准入信号在 iOS 的等价实现：

| 配置 | Android | iOS |
|---|---|---|
| `requireWifi` | ConnectivityManager | `NWPathMonitor`：`usesInterfaceType(.wifi)`（iOS 不区分家宽/热点，判定粒度粗于 Android，产品约束明示） |
| `requireCharging` | BatteryManager | `UIDevice.batteryState ∈ {charging, full}` |
| 低电量 | BatteryManager | `batteryLevel` 低于阈值 **或** 低功耗模式开启（取更严者） |

后台任务编排（对照 §5.3）：

- **`BGAppRefreshTask`**（`…refresh`）：机会性唤醒，跑一轮「实时通道补投 + INBOX 回放」，完成即 `setTaskCompleted`；预算由系统控制（典型每日数十次），不做重补传。
- **`BGProcessingTask`**（`…backfill`，`requiresNetworkConnectivity + requiresExternalPower`）：与补传准入（WiFi+充电）**语义天然一致**，系统只在条件满足时调度；唤醒后经 bridge 触发 headless JS 补传循环（同 Android WorkManager 路径 B），**每次任务 ≤2 批**即完成（预算友好，§5.3 约束）。
- **注册时序硬约束**：BGTask launch handler 必须在宿主 `didFinishLaunching` 返回前注册——turbo-module 按需初始化无法自行满足 → SDK 提供注册入口（`registerBackgroundTasks`）供宿主 AppDelegate 显式调用（README 集成项；Expo 场景走 config plugin）。未注册时后台补传不可用，仅剩前台路径，启动时发 `error(fatal=false)` 告知。
- **进入后台**：监听 `applicationDidEnterBackground` → `beginBackgroundTask`（~30s）尽力完成在途批；未完成按失败退避。
- **后台 URLSession 限用于补传通道**（§5.3）：后台窗口大批传输用 background `NSURLSession`（请求体仍是 TS Encoder 字节输出，D11 不破坏；实时通道保持 JS `fetch`）。后台会话回调时 JS 可能已挂起 → **原生按 §4.3 不变式直接收尾**（点行标 SENT、batch 台账更新），事件缓存待 JS 恢复补发；响应中的 `nextUploadIntervalSec` 解析与 clamp（[10,300]）同在该回调完成，服务端调速语义不变。App 冷启动时 `handleEventsForBackgroundURLSession` 需宿主转发给 SDK（README 集成项）。

### 10.7 平台差异对照（产品与测试口径）

| 维度 | Android | iOS |
|---|---|---|
| 持续采集 | 前台服务（常驻通知） | 后台 location 模式（Always，状态栏胶囊） |
| 实时上报（后台） | JS 定时器持续有效 | 仅 ~30s 后台短窗口，未完成进退避 |
| 补传 | WorkManager 周期 ≥15min（可预期） | BGTask 机会性（时机不可承诺，R1） |
| 围栏上限 | 100 | 20 |
| 运动识别 | ActivityRecognition | CMMotionActivityManager（可被拒 → 速度推断降级） |
| WiFi 判定 | 可区分计费/非计费 | 仅接口类型，粒度粗 |
| 积压恢复 | 离线 24h 恢复零丢失（§8） | 后台 8h 基线（§8），残余待前台清空（R1） |

### 10.8 podspec 与工程配置

- 链接系统组件：frameworks `CoreLocation`、`CoreMotion`、`BackgroundTasks`、`UIKit`；libraries `libsqlite3.tbd`；deployment target 跟随 RN（`min_ios_version_supported`）。全部系统框架，无第三方 pod。
- `Flowtrace.podspec` 的 `source_files = "ios/**/*.{h,m,mm,swift,cpp}"` 已覆盖后续新增的 `FT*.mm`，无需改结构。
- 模拟器限制：`CMMotionActivityManager` 在模拟器无数据（运动识别仅真机可测）、BGTask 调度时机不可控——集成回归（§8）用真机。
