# Zeta-spark-service

一个极薄的 Spark SQL HTTP 服务：容器里常驻一个 SparkSession（Local 模式），提供健康检查、执行 SQL、取消在途查询三个接口。库表数据持久化在数据卷里，重启不丢。

本服务是为 [Zeta](https://github.com/WorkerHub/Zeta) 支持 SparkSQL 而开发的**可选组件**：Zeta 本身不内置 Spark 引擎，不用 SparkSQL 就无需部署；要用时部署一个本服务作为后端（一台实例对应 Zeta 里登记的一个 Spark 数据源）。推荐部署平台 **Instabox**。

- Spark 3.5.3，Local 模式（driver 与 executor 同进程），开启 Hive 支持；元数据存内嵌 Derby，托管表数据以 Parquet 存 warehouse（非 HDFS）
- 令牌鉴权；多会话隔离（各自的当前库与临时视图互不干扰）

## 快速开始

### docker compose（推荐）

```bash
git clone https://github.com/WorkerHub/Zeta-spark-service.git
cd Zeta-spark-service
cp .env.example .env   # 把 SPARK_API_TOKEN 改成你自己的随机令牌
docker compose up -d
```

### docker run

```bash
docker run -d --name spark-sql-service \
  -p 127.0.0.1:8080:8080 \
  -e SPARK_API_TOKEN=你的令牌 \
  -v spark-data:/data/spark \
  workerhub/zeta-spark-service:latest
```

本地构建：`docker build -t spark-sql-service .`

## 接入 Zeta

在 Instabox 上部署（一台机器一个实例，4 vCPU / 8 GB 足够），服务起好后：

1. **发布接口**：在机器终端里执行 `instabox-port publish 8080 --private`，把 `8080` 发布为 private 端口（控制台 UI 只能发布 public 端口，private 必须用这条命令）。命令会当场打印一条带 `?_token=ibp_…` 的链接，只显示一次、当场保存：链接去掉 `?_token=` 的部分是**发布 URL**，`ibp_…` 就是**平台令牌**。
2. **在 Zeta 里登记**：新增 Spark 数据源，填三项——发布 URL、平台令牌、应用令牌，保存后点「连接」验证。
   - **平台令牌**：Zeta 以 `Authorization: Bearer` 发送，由 Instabox 发布代理校验。**自托管（VPS 等）没有平台网关，这一项填任意随机字符串占位即可**（如 `openssl rand -hex 32`），实际把关的是应用令牌。
   - **应用令牌**：你自己设定的 `SPARK_API_TOKEN`（部署目录 `.env` 里），Zeta 以 `X-Auth-Token` 发送，由本服务校验。

自托管（VPS 等）同理：给服务套一个公网可达的 HTTPS 入口（如 Caddy 反代或 Cloudflare Tunnel），compose 仍只绑 `127.0.0.1`，不要把 8080 裸奔公网——本服务能执行任意 SQL。

## 接口

### `GET /health`

```json
{"ok": true, "sparkVersion": "3.5.3"}
```

自愈：内置看门狗每 30 秒探活 Spark 网关，JVM 已死时连续两次探活失败即退出进程，由 Docker 重启策略拉起恢复（会话状态清空，metastore 数据不受影响）。

### `POST /query`

请求头 `X-Auth-Token: <你的令牌>`，请求体：

```json
{"sql": "select 1", "maxRows": 1000, "session": "alice"}
```

| 字段 | 说明 |
|---|---|
| `sql` | 单条 SQL（一次一句） |
| `maxRows` | 最多返回行数，硬上限 5000 |
| `session` | 可选。多会话标识（≤128 字符）：同标识共享当前库、临时视图与会话配置，不同标识互相隔离；库表（metastore）全会话共享。不传则走默认会话 |
| `execution` | 可选。执行 ID（≤128 字符），与 `/cancel` 配合按次取消 |

成功返回 `{"columns": ["1"], "rows": [[1]], "rowCount": 1, "truncated": false}`。错误形态：令牌缺失/错误 → 401 `{"error": "unauthorized"}`；请求体不合法 → 400；SQL 执行出错 → 200 + `{"error": "..."}`。

### `POST /cancel`

取消在途查询。请求头同 `/query`，请求体 `{"session": "alice", "execution": "550e8400-..."}`（两字段均可选，缺省走默认会话）：

- 提供 `execution` 时只取消该次执行的 job group（`session:execution`），迟到的取消不误杀同会话下一条执行；不提供则按会话级取消
- 实现：执行时按会话（或会话:执行）设置 Spark job group，取消时另一线程调 `cancelJobGroup`；会话与 SparkSession 不销毁，临时视图与当前库保留
- 无在途 job 时同样返回 `{"ok": true}`（幂等）；被取消的查询以 200 + `{"error": "… cancelled …"}` 返回；鉴权与请求体规则同 `/query`

试一句：

```bash
curl -s -X POST http://127.0.0.1:8080/query \
  -H "X-Auth-Token: 你的令牌" \
  -H "Content-Type: application/json" \
  -d '{"sql": "create database if not exists demo"}'
```

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `SPARK_API_TOKEN` | （空） | **必填**。未设置时所有接口（`/query`、`/cancel`）一律 401 |
| `PORT` | `8080` | 监听端口 |
| `SPARK_DATA_DIR` | `/data/spark` | metastore 与 warehouse 所在目录，挂卷到这里 |
| `SPARK_MASTER` | `local[*]` | Spark master（本地模式线程数） |
| `SPARK_DRIVER_MEMORY` | `2g` | driver 内存，按机器大小调 |
| `SPARK_APP_NAME` | `spark-sql-service` | Spark 应用名 |

## 数据与安全

数据目录（默认 `/data/spark`）挂到持久卷上即可长期保留，删卷 = 库表全丢：

- `metastore_db/`：Derby 数据库，存库/表定义
- `warehouse/`：托管表数据，一库一子目录、一表一子目录，Parquet 格式
- `tmp/`：Spark 本地临时目录

临时视图与会话状态只在内存里，服务重启即清（库表不受影响）。令牌是唯一的门：用随机长令牌，不要提交进 git；对外只走带鉴权的反向代理或隧道。

## 说明

单机练习/轻量场景够用，不是集群方案——要 Standalone/YARN 集群、Kyuubi/JDBC 网关或多人生产隔离，这个薄服务都不适合。

## License

[MIT](LICENSE)
