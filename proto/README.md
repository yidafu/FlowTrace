# proto/

轨迹上报协议定义（Protobuf + Delta 编码）。

约定（详见仓库根目录 AGENTS.md「关键设计约定」一节）：

- 上报编码只用 Protobuf + Delta + GZIP，不用 JSON；
- 批头放基准值（base ts/lat/lng），批内点字段做增量编码；
- 坐标整数量化（×1e7），Protobuf 序列化后整体 GZIP；
- SDK `transport/` 与服务端解码保持同一约定；协议改动需同步服务端、SDK、存储三端兼容性。

## 文件

- `trace.proto`：对外上报协议（`flowtrace.v1`），SDK 与服务端共用。
- `trace_message.proto`：Kafka 内部消息（`flowtrace.internal.v1`，点为绝对坐标），仅服务端（upload 写入 / processor 消费），不进 SDK。

## 代码生成

本目录是协议唯一事实源，两端直接指向这里生成，不做文件复制：

- **后端**：`libs/trace-protocol` 经 protobuf-gradle-plugin 生成 Java + Kotlin DSL，随 `./gradlew build` 自动执行；protoc 与运行时版本统一在 `backend/gradle/libs.versions.toml`。
- **SDK**：`cd sdk/react-native-flowtrace && yarn proto:generate`（buf + Protobuf ES，输出 `src/transport/generated/trace_pb.ts`，生成代码随源码入库）。
