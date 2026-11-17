# react-native-flowtrace

FlowTrace 轨迹采集与上报 React Native SDK。

负责 App 端轨迹链路的端侧部分：**定位采集 → 本地队列缓存 → 批量压缩上报**，为巡检等业务提供轨迹佐证。SDK 只做轨迹采集与上报，**不承担巡检业务逻辑**——巡检业务通过 `taskId/sessionId` 关联消费轨迹结果，不侵入上报主链路。

## 当前状态

工程骨架期（2026-11-17 由 create-react-native-library 0.64 初始化）：

- ✅ 标准 RN 库工程（turbo-module，Android Kotlin + iOS Objective-C，bob 构建，codegen 包名 `dev.yidafu.flowtrace`）
- ✅ 上报协议生成链路：`proto/trace.proto` → buf + protobuf-es → `src/transport/generated/trace_pb.ts`（生成代码随源码入库）
- 🚧 核心模块开发中：`src/core/`（采样、批量、重试）、`src/storage/`（SQLite 本地队列）、`src/location/`（定位抽象）、`src/ui/`（轨迹管理页面，可选子入口）
- `src/index.tsx` 目前仍是脚手架占位代码（multiply 示例），公开 API 待核心模块落地后提供

## 安装

```sh
yarn add react-native-flowtrace
```

> 包尚未发布到 npm；发布前请在 monorepo 内通过 workspace 引用（参考 `example/`）。

## 上报协议

与 FlowTrace 服务端共用同一协议事实源（仓库根目录 `proto/`），编码链路统一约定为 **Protobuf + Delta + GZIP**，不用 JSON：

- **批头基准值**：`TraceBatchHeader.base` 放批首点绝对值（ts 毫秒、经纬度 ×1e7 整数量化），批内点（`TracePointDelta`）做链式增量编码（`cur = prev + delta`）
- **双通道**：`CHANNEL_REALTIME`（实时，小批 ≤50 点，任意可用网络）/ `CHANNEL_BACKFILL`（补传，大批 ≤500 点，WiFi+充电），互不阻塞
- **幂等**：`batch_id`（UUIDv7）全局唯一作为幂等键，服务端唯一索引防重
- **序列化后整体 GZIP**，`POST /v1/traces/batch` 上报，Bearer token 鉴权

协议细节见 [proto/trace.proto](../../proto/trace.proto)；TS 类型由 `yarn proto:generate` 生成到 `src/transport/generated/`。

## 目录结构

```
src/
├── core/                  # 采样、批量、重试（开发中）
├── storage/               # 本地队列抽象（SQLite，开发中）
├── transport/
│   └── generated/         # Protobuf ES 生成代码（随源码入库，不手改）
├── location/              # 定位抽象（开发中）
├── ui/                    # 轨迹管理页面（可选子入口 ./ui，MapLibre，开发中）
├── NativeFlowtrace.ts     # TurboModule spec
└── index.tsx              # 对外导出
android/                   # 原生 Android 模块（Kotlin，含 Room）
ios/                       # 原生 iOS 模块（Objective-C）
example/                   # 示例 App
```

## 环境要求

- React Native 需启用 New Architecture（turbo-module；本仓库以 RN 0.88 + React 19 开发验证）
- Android minSdk 24
- iOS 部署目标跟随 RN 模板默认值（`Flowtrace.podspec` 使用 `min_ios_version_supported`）

## 开发

SDK 位于 FlowTrace monorepo 的 `sdk/react-native-flowtrace/`，使用 yarn 4 workspace。

```sh
yarn install            # 安装依赖
yarn prepare            # bob 构建 → lib/（gitignore，不手改）
yarn typecheck          # TS 类型检查
yarn lint               # ESLint
yarn test               # Jest
yarn proto:generate     # Protobuf TS 生成（buf + protobuf-es）
yarn example ios        # 运行示例 App（iOS）
yarn example android    # 运行示例 App（Android）
```

> 协议文件改动不要直接编辑 `src/transport/generated/`，改仓库根目录 `proto/` 后重新生成。涉及协议或数据模型的改动需同步评估服务端、SDK、存储三端兼容性。

## Contributing

- [Development workflow](CONTRIBUTING.md#development-workflow)
- [Sending a pull request](CONTRIBUTING.md#sending-a-pull-request)
- [Code of conduct](CODE_OF_CONDUCT.md)

## License

MIT
