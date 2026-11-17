# 04 API 设计

> 上游：[02-协议与编码](02-protocol.md)、[03-存储](03-storage.md)｜ 落地位置：flowtrace-upload / flowtrace-query / realtime(并入 query) / flowtrace-admin

## 1. 通用约定

| 项 | 约定 |
|---|---|
| Base path | `/v1`（破坏性变更升 `/v2`，`/v1` 至少保留 2 个大版本周期） |
| 鉴权 | `Authorization: Bearer <JWT>`（RS256）。claims：`sub`（userId，数字字符串）、`did`（deviceId，上报必带）、`scope`（空格分隔：`trace:write` / `trace:read` / `trace:read:any` / `trace:admin`）、`exp ≤ 24h`。JWKS 按 issuer 配置发现，缓存 5min |
| 内容类型 | 上报：`application/x-protobuf` + `Content-Encoding: gzip`；其余 `application/json; charset=utf-8` |
| 时间参数 | 查询参数 ISO-8601 UTC（`2026-11-17T00:00:00Z`）；`end` **独占**；响应时间戳一律 epoch 毫秒 |
| 分页 | 游标式：响应 `nextCursor`（base64url，不透明），请求传回 `cursor`；默认 `limit=1000`（各接口上限见下） |
| 请求追踪 | 服务端回 `X-FlowTrace-Trace-Id`（W3C traceparent 派生），错误体携带同值 |
| 幂等 | `POST /v1/traces/batch` 幂等（batchId）；`DELETE` 作业幂等（重复触发返回进行中/已完成作业）；其余 GET 天然幂等 |

### 1.4 错误体

```json
{
  "error": {
    "code": "RATE_LIMITED",
    "message": "设备批次数超限",
    "retryAfterSec": 60,
    "traceId": "00-4bf9...-01",
    "details": { "nextUploadIntervalSec": 300 }
  }
}
```

错误码全集：`UNAUTHORIZED`、`FORBIDDEN`、`DEVICE_DISABLED`、`INVALID_GZIP`、`INVALID_PROTOBUF`、`INVALID_SCHEMA_VERSION`、`TS_OUT_OF_WINDOW`、`INVALID_COORD`、`TS_NOT_SORTED`、`PROTOCOL_ERROR`、`PAYLOAD_TOO_LARGE`、`RATE_LIMITED`、`NOT_FOUND`、`VALIDATION_ERROR`、`CONFLICT`、`INTERNAL`。

### 1.5 权限矩阵

| 接口组 | scope 要求 | 额外约束 |
|---|---|---|
| `POST /v1/traces/batch` | `trace:write` | token `did` 必须等于 body `device_id`；`sub` 必须等于 `user_id` |
| 查询组（`/v1/traces*`、`/v1/users/{id}/location`） | `trace:read` | 用户 token 仅允许 `sub == path/query userId`；服务 token 需 `trace:read:any` |
| `DELETE /v1/users/{id}/traces` | `trace:admin` | — |
| `/v1/admin/*` | `trace:admin` | — |

## 2. 上报

### `POST /v1/traces/batch`

请求（ Protobuf 定义见 02 §3，此处只列 HTTP 层）：

| 头 | 值 |
|---|---|
| `Authorization` | `Bearer <JWT>`（含 `trace:write`、`did`） |
| `Content-Type` | `application/x-protobuf` |
| `Content-Encoding` | `gzip`（缺失按 `INVALID_GZIP` 拒；不解压明文） |
| `Content-Length` | ≤ 256KB（网关层） |

服务端处理链（细节见 06 §3）：鉴权 → 限流 → 解压（≤2MB）→ 解码 → 字段校验 → 幂等 → Kafka → 响应。

**200 响应**：

```json
{
  "batchId": "018f3a2e-...",
  "accepted": 50,
  "deduplicated": false,
  "nextUploadIntervalSec": 30,
  "serverTsMs": 1763376000000
}
```

- `deduplicated=true`：batchId 命中幂等（未重写 Kafka），SDK 按成功处理。
- `accepted`：进入 Kafka 的点数（批内折叠的完全重复点不计）。
- 422 = 确定性失败（编码/校验错误），SDK **不重试**，整批丢弃并计指标（02 §9）；`details.rejectedPoints: [{idx, reason}]` 尽力给出。
- 429：`Retry-After` 头 + `details.nextUploadIntervalSec=300`。

### 2.3 动态调速算法（`nextUploadIntervalSec`）

服务端无状态计算（输入为 Redis 限流计数，不引入新存储）：

```text
if cfg:upint:{deviceId} 存在        -> 返回该值（运维覆盖，07 配置项 UPLOAD_INTERVAL_OVERRIDE）
p = rl:pc:{deviceId} 当前分钟点数
base = 30
if  p >= 0.9 * pointsPerMinLimit    -> 120      # 默认 6000 点/分钟 -> p >= 5400
elif p >= 0.7 * pointsPerMinLimit   -> 60
if 本次触发限流 (429)               -> 300
clamp [10, 300]
```

SDK 侧同样钳制 `[10, 300]` 并在每次 200 响应后更新本地值（05 §6.2）。

## 3. 查询组（flowtrace-query）

### `GET /v1/traces` — 轨迹点查询（默认简化）

参数：`userId`(必填)、`start`/`end`(必填，跨度 ≤ 7 天)、`simplify=true`(默认；`false` 返回原始点)、`zoom`(0..21，默认 15，简化 LUT 见 06 §5.3)、`maxPoints`(默认 2000，简化后仍超则等距抽取)、`format=geojson|compact`(默认 `geojson`)、`limit`(默认 2000，≤5000)、`cursor`。

响应（geojson）：

```json
{
  "userId": 10001,
  "start": "2026-11-17T00:00:00Z", "end": "2026-11-18T00:00:00Z",
  "simplify": true, "zoom": 15, "format": "geojson",
  "geometry": { "type": "LineString", "coordinates": [[116.1234567, 39.1234567], ...] },
  "timestampsMs": [1763376000000, ...],
  "returnedPoints": 1820, "originalPoints": 9412,
  "nextCursor": "bGltaXQ9MjAwMCZ0cz0xNzY...", "hasMore": false
}
```

- `compact` 格式：`"points": [[tsMs, latE7, lngE7], ...]`（整数三元组，客户端按 `/1e7` 还原；省流量场景推荐）。
- `simplify=false` 时忽略 `zoom/maxPoints`，返回原始点（仍受 `limit` 分页约束）。
- 坐标输出十进制度 double，精度由量化决定（≤7 位小数语义）。

### `GET /v1/trace-segments` — 轨迹段/停留点

参数：`userId`(必填)、`start`/`end`(必填，跨度 ≤ 31 天)、`kind=move|stay|all`(默认 all)、`sessionId`/`taskId`(可选过滤)、`format=wkt|geojson`(默认 wkt)、`limit`(默认 500)、`cursor`。

```json
{
  "segments": [
    {
      "segmentId": "9af0...", "kind": "MOVE", "moveMode": "WALK",
      "startTsMs": 1763376000000, "endTsMs": 1763379600000, "durationSec": 3600,
      "pointCount": 612, "distanceM": 4213.5,
      "geometry": "LINESTRING(116.1234567 39.1234567, ...)",
      "bbox": { "minLat": 39.12, "minLng": 116.12, "maxLat": 39.13, "maxLng": 116.14 },
      "status": "FINALIZED"
    },
    {
      "segmentId": "5c31...", "kind": "STAY",
      "startTsMs": 1763379600000, "endTsMs": 1763381400000, "durationSec": 1800,
      "pointCount": 4, "distanceM": 0,
      "geometry": "POINT(116.125 39.125)", "stayLat": 39.125, "stayLng": 116.125
    }
  ],
  "nextCursor": null
}
```

### `GET /v1/traces/playback` — 回放（按缩放级别取点）

参数：`userId`、`start`/`end`（跨度 ≤ 24h）、`zoom`（默认 15）、`maxPoints`（默认 500）。等价于 `GET /v1/traces` 的简化管道 + 回放专用 LUT（06 §5.4），返回 `compact` 数组 + 每点 `speed/bearing` 可选字段（`withAttrs=true`）。

### `GET /v1/traces/distance` — 里程

参数：`userId`、`start`/`end`（跨度 ≤ 31 天）、`byMode=true`(默认)。

```json
{ "totalM": 18342.7, "byMode": { "WALK": 4213.5, "VEHICLE": 14129.2 }, "source": "segments" }
```

口径：段表求和（`kind=MOVE`）；目标范围内存在 `status=OPEN` 或缺失段（如早期数据）时回退为该范围原始点逐段 haversine 计算，`source="points"`，响应头 `X-FlowTrace-Degraded: true`。

### `GET /v1/users/{userId}/location` — 最新位置（Redis）

```json
{
  "userId": 10001, "deviceId": "dev-abc", "tsMs": 1763375999123,
  "lat": 39.1234567, "lng": 116.1234567,
  "accuracyCm": 800, "speedMps": 1.3, "bearingDeg": 45,
  "updatedAtMs": 1763375999900, "stale": false
}
```

`stale = updatedAt 距服务端当前时间 > 5min`（`STALE_MS=300000`）。无数据 → 404 `NOT_FOUND`。

## 4. 删除与作业

### `DELETE /v1/users/{userId}/traces`

参数：`start`/`end`（可选，缺省全量删除）。→ `202 {"jobId": "j-...", "status": "QUEUED"}`。执行内容与覆盖矩阵见 03 §10；重复触发幂等（同用户存在运行中作业则返回该 jobId）。

### `GET /v1/jobs/{jobId}`

```json
{
  "jobId": "j-...", "type": "USER_TRACES_DELETE", "status": "RUNNING",
  "progress": { "totalPartitions": 30, "donePartitions": 12, "deletedPoints": 1284503 },
  "createdAtMs": 1763376000000, "finishedAtMs": null, "error": null
}
```

status：`QUEUED / RUNNING / DONE / FAILED(可重跑)`。type 还包括 `REPROCESS`、`ARCHIVE_RUN`。

## 5. 管理组（flowtrace-admin，scope=`trace:admin`）

| 接口 | 说明 |
|---|---|
| `POST /v1/admin/devices` | 预注册设备 `{deviceId, userId, platform}`（可选流程；未注册设备首次合法上报自动注册为 ACTIVE） |
| `GET /v1/admin/devices?userId=&status=&cursor=` | 设备列表 |
| `PATCH /v1/admin/devices/{deviceId}` | `{status}`：`SUSPENDED`/`DISABLED` 后 upload 拒收（设备状态缓存 TTL 60s，见 06 §3.2）；恢复 `ACTIVE` |
| `GET /v1/admin/retention-policies` / `PUT`（同 body） | 查询/更新保留策略（scope=GLOBAL 或 USER 覆盖，结构见 03 §5） |
| `POST /v1/admin/reprocess` | `{type: "user-day", userId, day}` 或 `{type: "dlq", batchIds: [...]}` → jobId（语义见 06 §4.6/§8） |
| `GET /v1/admin/dlq/batches?cursor=&reason=` | 死信批列表（batchId、device、错误、时间） |
| `GET /v1/admin/audit?actor=&action=&start=&end=&cursor=` | 审计查询 |

## 6. 一致性与边界语义（对业务方承诺）

| 场景 | 行为 |
|---|---|
| 点可查延迟 | 上报成功后 ≤ 1min（正常网络）；最新位置走 `/location`，秒级 |
| 实时与历史一致性 | `GET /v1/traces` 可能暂缺最后 ≤1min 的点；回放请结合 `/location` 补最新点 |
| 段与点不一致窗口 | 段由异步管道产出：点的 `status=OPEN` 段边界可能随后续点变化（05/06 水位机制），`FINALIZED` 段不再变化 |
| 删除期间查询 | 删除作业进行中数据逐步消失，不保证原子；作业完成后 7 天内 Parquet 副本清理完成 |
| 补传历史数据 | 入库后立即可查（点）；段按 `dirty_day` 重算后更新（分钟级延迟） |
