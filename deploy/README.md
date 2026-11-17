# deploy

本地开发依赖编排（podman compose）。

## 依赖服务

| 服务 | 镜像 | 宿主机地址 | 容器网络地址 | 用途 |
| --- | --- | --- | --- | --- |
| Kafka | apache/kafka:3.9.0（KRaft 单节点） | `localhost:9092` | `kafka:29092` | 轨迹批次消息（topic `trace.batch.v1`，key=deviceId） |
| Redis | redis:7-alpine（AOF 开启） | `localhost:6379` | `redis:6379` | 最新位置 `rt:pos:*`、批幂等 `idem:batch:*`、限流 |

## 启动 / 停止

```bash
# macOS 首次或重启后需保证 podman machine 在运行
podman machine start

cd deploy
podman compose up -d      # 拉起
podman compose ps         # 查看状态（等 healthy）
podman compose down       # 停止，保留数据卷
podman compose down -v    # 停止并清空 Kafka/Redis 数据
```

## 验证

```bash
# Kafka：列出 topic（首次被服务写入时自动创建 trace.batch.v1，默认 8 分区）
podman exec flowtrace-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Redis
podman exec flowtrace-redis redis-cli ping   # PONG
```

## 说明

- Kafka 为 KRaft 单节点（broker+controller 合一），无 ZooKeeper；cluster id 由镜像首次格式化时生成并持久化在 `kafka-data` 卷。
- 开发环境自动建 topic、默认 8 分区；生产 32 分区拓扑见 `docs/design/01-overview.md` §8。
- PostgreSQL 不在本 compose 内（表结构由 Exposed `SchemaUtils` 管理，另行本地安装或后续按需补充）。
