# 02 协议与编码设计

> 上游：[01-总体设计](01-overview.md)（决策 D1~D5、D11）｜ 落地位置：`proto/trace.proto`、`proto/trace_message.proto`、`proto/testdata/`

本文档定义端到端的字节级约定：**SDK（TS）编码 → HTTP 传输 → flowtrace-upload（Kotlin）解码 → Kafka 内部消息**。本文的 proto 定义即实现规格，落地时原样生成文件（可加注释、不得改字段号与语义）。

## 1. 版本与兼容策略

- `schema_version` 当前为 `"1.0"`。服务端按 `主.次` 比较：主版本不同直接拒收（`INVALID_SCHEMA_VERSION`）；次版本不同接受（向前兼容）。
- proto3 语义：未知字段解码时**保留不丢**；双方只允许**新增**字段（使用新字段号），不得复用/修改既有字段号与类型，不得改变既有字段语义；废弃字段标 `deprecated` 注释，永不删除。
- 对外协议（`trace.proto`）与内部消息（`trace_message.proto`）独立演进。内部消息破坏性变更时新开 topic `trace.batch.v2`，双写过渡，不做在线反序列化兼容。
- 兼容性红线（AGENTS.md）：协议改动需同步服务端、SDK、存储三端评审。

## 2. 坐标与时间表示（决策 D1）

| 量 | 表示 | 范围/精度 |
|---|---|---|
| 纬度 | `lat_q` = round(度 × 10⁷) | ±900_000_000，精度 ≈1.1cm，存储 int32/sint64 |
| 经度 | `lng_q` = round(度 × 10⁷) | ±1_800_000_000，同上 |
| 时间 | epoch **毫秒** int64 | 2001~2286 年，足够 |
| 量化规则 | 半数舍入到偶数避免系统偏置：`(int) Math.round(IEEE754 度值 * 1e7)` | TS 用 `Math.round`，双端一致（见 §8 向量） |
| 反量化 | `lat_q / 1e7`（仅展示层做；服务端内部一律保持整数） | — |

坐标字段在 proto 中一律用 **sint64**（zigzag）：绝对值虽在 int32 范围内，但负数在 int32 下编码占 10 字节，zigzag 下 5 字节，且给 Delta 留出溢出余量（解码后做范围校验兜底）。

## 3. 上报协议 `proto/trace.proto`

```protobuf
syntax = "proto3";
package flowtrace.v1;

// 上报通道
enum Channel {
  CHANNEL_UNSPECIFIED = 0;  // 非法，拒收
  CHANNEL_REALTIME = 1;     // 实时通道（小批 ≤50 点，任意网络）
  CHANNEL_BACKFILL = 2;     // 补传通道（大批 ≤500 点，WiFi+充电）
}

// 定位来源
enum LocationSource {
  SOURCE_UNSPECIFIED = 0;
  SOURCE_GPS = 1;
  SOURCE_NETWORK = 2;
  SOURCE_FUSED = 3;         // 缺省按 FUSED 处理
  SOURCE_MANUAL = 4;        // 用户手动打点/纠偏
}

// 点级标志位（bit 掩码，可组合）
// 0x01 FLAG_ENTER_RADIUS   进入巡检点/围栏半径（关键点，触发实时上报）
// 0x02 FLAG_EXIT_RADIUS    离开半径
// 0x04 FLAG_MOTION_CHANGE  运动状态切换
// 0x08 FLAG_FORCED         强制保留点（关键点优先策略）
// 0x10 FLAG_LOW_BATTERY    低电量模式下采集

// Delta 基准：本批第一个点的绝对值
message GeoBase {
  int64 ts_ms = 1;   // epoch 毫秒
  sint64 lat_q = 2;  // 纬度 ×1e7
  sint64 lng_q = 3;  // 经度 ×1e7
}

message SdkInfo {
  string sdk_version = 1;   // 语义化版本，如 "1.2.0"
  string app_version = 2;   // 宿主 App 版本
  string platform = 3;      // "android" | "ios"
  string os_version = 4;    // 系统版本，如 "17.4" / "34"
  string model = 5;         // 机型（可选，如 "Pixel 8"）
}

message TraceBatchHeader {
  string batch_id = 1;       // 【必填】UUIDv7，全局唯一，幂等键（§7）
  string schema_version = 2; // 【必填】"1.0"
  string device_id = 3;      // 【必填】必须与 token did 声明一致
  string user_id = 4;        // 【必填】必须与 token sub 一致，否则拒收
  string session_id = 5;     // 【可选】采集会话 ID（SDK start() 生成 UUIDv7）
  string task_id = 6;        // 【可选】业务任务关联：透传存储，不做语义校验
  GeoBase base = 7;          // 【必填】Delta 基准 = 本批首点绝对值
  Channel channel = 8;       // 【必填】
  SdkInfo sdk = 9;           // 【必填】
  int64 client_ts_ms = 10;   // 【必填】编码时刻
}

// 批内点：相对前一点的链式增量（决策 D2）
message TracePointDelta {
  sint64 d_ts = 1;           // ts_ms  差；首点 = 0（proto3 省略）
  sint64 d_lat = 2;          // lat_q  差；首点 = 0
  sint64 d_lng = 3;          // lng_q  差；首点 = 0
  int32 accuracy_cm = 4;     // 【可选】定位精度 cm（68% 置信）
  float speed_mps = 5;       // 【可选】速度 m/s
  int32 bearing_deg = 6;     // 【可选】航向 0..359
  int32 altitude_cm = 7;     // 【可选】海拔 cm（WGS84）
  LocationSource source = 8; // 【可选】缺省 FUSED
  uint32 flags = 9;          // 【可选】位掩码（见上）
}

message TraceBatchRequest {
  TraceBatchHeader header = 1;
  repeated TracePointDelta points = 2;  // 按 ts 升序，1..500 个（上限见 §5）
}
```

## 4. Delta 编解码规范（决策 D2）

### 4.1 语义

- `header.base` = 本批**第一个点**的绝对值 `(ts, lat_q, lng_q)`。
- `points[i]` 的三个增量字段是**相对解码后的前一个点**的差：`cur = prev + delta`，`prev₀ = base`。
- 因此 `points[0]` 的 `d_ts/d_lat/d_lng` 恒为 0，proto3 零值省略——首点几乎不占字节。
- 批自包含：解码只依赖本批字节，无跨批状态 → 服务端无状态、重试安全。

### 4.2 编码（SDK，TS）

```text
encode(points):            // 前置：points 已按 ts 升序排序（不变式，见 05 §5.3）
  assert points.length in 1..500
  header.base = points[0] 的 (ts, lat_q, lng_q)
  prev = points[0]
  for i in 1..n-1:
    d[i] = (points[i].ts - prev.ts, points[i].lat_q - prev.lat_q, points[i].lng_q - prev.lng_q)
    prev = points[i]
  序列化 TraceBatchRequest → gzip(level 6)
```

### 4.3 解码（服务端，Kotlin，`libs/trace-protocol`）

```text
decode(msg):
  if msg.points.isEmpty -> PROTOCOL_ERROR("空批")
  prev = (msg.header.base.ts_ms, base.lat_q, base.lng_q)
  out = []
  for (i, p) in points:
    ts  = prev.ts  + p.d_ts          // d_ts 必须 >= 0（编码端已排序）；违例 -> TS_NOT_SORTED，整批 422
    lat = prev.lat + p.d_lat
    lng = prev.lng + p.d_lng
    require lat in [-900_000_000, 900_000_000]        -> INVALID_COORD
    require lng in [-1_800_000_000, 1_800_000_000]    -> INVALID_COORD
    out.add(AbsolutePoint(idx=i, ts, lat, lng, attrs(p)))
    prev = cur
  // 批内二级去重：相邻 (ts, lat, lng) 完全相同 -> 折叠并计数，不报错
  return out
```

溢出说明：`sint64` 增量解码后 int64 不会溢出；越界的绝对坐标由范围校验拒绝。Delta 链上单点错误只影响该点之后（不传播——每点增量是相对**真实前点**，而真实前点由链推出，故解码失败即整批失败，安全语义：**批是校验与成败的最小单位**）。

## 5. 传输封装与尺寸预算

HTTP（详见 [04-API](04-api.md) §2）：

| 项 | 约定 |
|---|---|
| 端点 | `POST /v1/traces/batch` |
| 头 | `Authorization: Bearer <JWT>`；`Content-Type: application/x-protobuf`；`Content-Encoding: gzip`（必须） |
| 压缩 | gzip（zlib），SDK 固定 level 6；服务端解压后大小上限 **2MB**（防压缩炸弹，超限 413） |
| 压缩前上限 | SDK 侧 1MB（编码器断言，超限拆批） |
| 点数上限 | REALTIME ≤ 50（服务端宽限 ≤100）；BACKFILL ≤ 500 |
| HTTP body 上限 | 网关 256KB（压缩后） |
| 响应 | `application/json`（上报响应不走 protobuf，见 04 §2.3） |

尺寸预算（估算，用于容量与断言基线）：

| 场景 | 点数 | 未压缩 | gzip 后 |
|---|---|---|---|
| 步行实时批 | 20 | ≈ 0.4KB | ≈ 0.3KB |
| 实时上限 | 50 | ≈ 0.9KB | ≈ 0.6KB |
| 驾车实时批（含 speed/bearing） | 50 | ≈ 1.5KB | ≈ 0.9KB |
| 补传上限 | 500 | ≈ 9KB | ≈ 5KB |

## 6. Kafka 内部消息 `proto/trace_message.proto`（决策 D4）

`flowtrace-upload` 解码校验通过后，发布到 `trace.batch.v1` 的消息体（**绝对值**，key = `deviceId`）：

```protobuf
syntax = "proto3";
package flowtrace.internal.v1;

// topic trace.batch.v1 消息 value；Kafka 头：
//   bId=batchId（十进制字符串，便于运维检索）
//   ch=1|2（通道）
message TraceBatchMessage {
  string batch_id = 1;
  string schema_version = 2;
  string device_id = 3;
  string user_id = 4;
  string session_id = 5;
  string task_id = 6;
  int32 channel = 7;          // 1 realtime / 2 backfill
  int64 received_ts_ms = 8;   // flowtrace-upload 接收时刻（对账/延迟分析用）
  repeated AbsolutePoint points = 9;
}

message AbsolutePoint {
  int64 ts_ms = 1;
  int32 lat_q = 2;
  int32 lng_q = 3;
  int32 accuracy_cm = 4;      // 0 = 缺省
  float speed_mps = 5;        // 0 = 缺省
  int32 bearing_deg = 6;      // 0 = 缺省
  int32 altitude_cm = 7;      // 0 = 缺省
  int32 source = 8;           // LocationSource 数值，0 = UNSPECIFIED
  uint32 flags = 9;
  int32 idx = 10;             // 批内序号（pointId 派生入参）
}
```

## 7. 标识派生与幂等不变式（决策 D3/D5）

| 标识 | 生成方 | 规则 | 生命周期 |
|---|---|---|---|
| `batchId` | SDK | UUIDv7（RFC 9562）。批成型时生成一次并**持久化到队列行**（05 §4.3），重试复用；上报成功后点行标记 SENT（本地保留窗口内供轨迹管理页面展示，05 §4.2），传输生命周期即告结束 | 一次传输生命周期 |
| `pointId` | 服务端（upload 或 processor） | `UUIDv5(FLOWTRACE_POINT_NS, "{batchId}:{idx}")`，`idx` 从 0 起 | 永久（落 `trace_point.point_id`） |
| `sessionId` | SDK | `start()` 时 UUIDv7 | 采集会话 |
| `segmentId` | processor | `UUIDv5(FLOWTRACE_SEGMENT_NS, "{userId}:{sessionId}:{start_ts_ms}")`（决策 D8） | 段 |

- `FLOWTRACE_POINT_NS` / `FLOWTRACE_SEGMENT_NS`：实现时定义为固定 UUID 常量，写入 `proto/trace.proto` 文件头注释与 `platform/common`（双端同值，纳入 golden vector 断言）。建议初始值：`FLOWTRACE_POINT_NS = 7d1a6b34-8f2c-4e5a-9b0d-1c2e3f4a5b6c`、`FLOWTRACE_SEGMENT_NS = 3e9f2a7c-6d4b-4c8e-8a1f-2b3c4d5e6f7a`（实现时可改，改后必须重跑向量）。

幂等不变式（违反任何一条即实现缺陷）：

1. 同一 `batchId` 至多被 `flowtrace-upload` 接受一次（Redis SETNX + `trace_batch` 唯一索引双保险，见 06 §3.4）。
2. 同一 `pointId` 至多落一行 `trace_point`（分区表唯一键 `(ts, point_id)`，ON CONFLICT DO NOTHING）。
3. 端侧批分配持久化：批一旦成型，其成员与顺序（idx）不再改变；重试发送**同一批字节**（重新编码结果与首次一致）。
4. 残余风险（SDK 本地库丢失后重组批）：由 processor 清洗规则「与前保留点 `(ts, lat, lng)` 完全相同 → 丢弃」吸收（06 §4.3）。

## 8. 一致性测试向量（golden vectors）

目的：TS 编码器与 Kotlin 解码器对同一组输入产出/接受**完全相同的字节与语义**（风险 R2）。CI 中双端必须全绿。

目录与格式：

```text
proto/testdata/v1/
  README.md                     # 格式说明与再生成步骤
  cases/
    single-point.bin/.json      # .bin  = gzip 后的 TraceBatchRequest 字节
    walk-20.bin/.json           # .json = 元信息 + 期望解码结果（绝对值数组）+ 期望 pointId 数组
    drive-50-full.bin/.json     #       + 期望 gzip 解压后 protobuf 字节的 sha256（锚定字节级一致）
    backfill-500.bin/.json
    negative-coords.bin/.json   # 南半球/西经
    dateline.bin/.json          # 跨 ±180° 经度（Delta 为大负数场景）
    high-latitudes.bin/.json    # ±80° 以上
    min-deltas.bin/.json        # 静止场景：全部 delta 为 0
  invalid/
    empty-points.bin            # 期望 422 + PROTOCOL_ERROR
    unsorted.bin                # d_ts < 0，期望 422 + TS_NOT_SORTED
    coord-overflow.bin          # 期望 422 + INVALID_COORD
```

- `.json` 结构：`{ "name", "description", "header": {...}, "expectedPoints": [{idx, tsMs, latQ, lngQ, ...}], "expectedPointIds": ["uuid", ...], "sha256ProtobufRaw" }`。
- 生成方式：由 `trace-protocol` 的测试工具从受控输入生成一次并入库；修改协议 = 新增向量 + 旧向量保留。
- 双端接入：SDK jest 直接读 `proto/testdata`；后端 Gradle test sourceSets 挂载 `proto/testdata` 为资源目录（06 §2.2）。

## 9. 错误语义（上报相关，完整错误体见 04 §1.4）

| HTTP | code | 触发 | SDK 侧动作（见 05 §6） |
|---|---|---|---|
| 200 | —（`deduplicated=true`） | `batchId` 重复 | 视为成功，删除本地批 |
| 400 | `INVALID_GZIP` / `INVALID_PROTOBUF` | 解压/反序列化失败 | 记指标；丢弃该批（本地毒丸隔离） |
| 401 | `UNAUTHORIZED` | token 无效/过期 | 触发 `tokenProvider` 刷新后重试一次 |
| 403 | `DEVICE_DISABLED` / `FORBIDDEN` | 设备被禁用/claim 不匹配 | 停止上报通道，通知宿主 App |
| 404/405 | — | — | 视为环境错误，走退避重试 |
| 413 | `PAYLOAD_TOO_LARGE` | 解压 >2MB / 点数超限 / body >256KB | 拆批或丢弃并计数 |
| 422 | `TS_OUT_OF_WINDOW` / `INVALID_COORD` / `TS_NOT_SORTED` / `PROTOCOL_ERROR` / `INVALID_SCHEMA_VERSION` | 字段校验失败 | 点级错误：丢弃错误点保留其余（若响应给出明细）；批级错误：丢弃整批 + 指标 |
| 429 | `RATE_LIMITED` | 触发限流（06 §3.5） | 按 `Retry-After` 暂停实时通道，`nextUploadIntervalSec` 生效 |
| 5xx | `INTERNAL` | 服务端异常 | 指数退避重试（通道互不阻塞） |
