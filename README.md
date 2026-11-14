# Zeta-spark-service

一个极薄的 Spark SQL HTTP 服务：容器里常驻一个 SparkSession（Local 模式），对外提供三个接口——健康检查、「发一句 SQL、拿回结果」和「取消在途查询」。建好的库表持久化在数据卷里，重启不丢。适合练 Spark SQL、给自己的小工具/应用当一个随开随用的 SparkSQL 后端。

**与 Zeta 的关系**：本服务是为 [Zeta](https://github.com/WorkerHub/Zeta) 支持 SparkSQL 而开发的**可选组件**——Zeta 本身不内置 Spark 引擎。不用 SparkSQL 就不需要部署它；要在 Zeta 里用 SparkSQL 数据源，就部署一个本服务作为它的后端（一台实例对应 Zeta 里登记的一个 Spark 数据源）。推荐部署平台是 **Instabox**（接入步骤见下文「接入 Zeta」）。

- Spark 3.5.3，Local 模式（driver 与 executor 同进程），开启 Hive 支持
- 元数据（库/表定义）存内嵌 Derby，托管表数据以 Parquet 存 warehouse——都在数据目录里，**不是 HDFS**
- 令牌鉴权；支持多会话隔离（一人一个会话，互不干扰当前库与临时视图）

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

镜像以 Docker Hub 为准（`workerhub/zeta-spark-service`）；GHCR 同步发布同一镜像（`ghcr.io/workerhub/zeta-spark-service`）。

## 接入 Zeta（部署步骤）

推荐在 **Instabox** 上部署：一台机器跑一个实例，规格 4 vCPU / 8 GB 足够。按「快速开始」把服务起好后，再做三件事：

1. **发布接口**：在 Instabox 控制台把这台机器的 `8080` 端口发布为 private 端口。发布后会得到一个**发布 URL** 和一个**平台令牌**——平台令牌只显示一次，当场保存好。
2. **备齐两个 key**（Zeta 登记时要填，一共三项：发布 URL + 两个令牌）：
   - **平台令牌（platform token）**：上一步发布端口时 Instabox 给出的那个。Zeta 每次请求都把它放在 `Authorization: Bearer` 头里，由 Instabox 的发布代理校验，请求到不了服务就先被它挡住。**自托管（自己的 VPS 等）没有平台网关、也就没有平台令牌**：这一项在 Zeta 里填任意随机字符串占位即可（Zeta 要求此项非空，随手生成一个，如 `openssl rand -hex 32`），不必在入口层再加一层校验——实际把关的是下面的应用令牌。
   - **应用令牌（application token）**：就是你自己设定的 `SPARK_API_TOKEN`，在部署目录的 `.env` 文件里（忘了就打开 .env 看）。Zeta 把它放在 `X-Auth-Token` 头里，由本服务校验。
3. **在 Zeta 里登记**：到 Zeta 的数据源管理新增一个 Spark 数据源，把发布 URL、平台令牌、应用令牌三项填进去，保存后点「连接」验证——通了就可以在 Zeta 里对这台实例跑 SparkSQL。

在其他平台（包括自己的 VPS）部署同理：给服务套一个公网可达的 HTTPS 入口（如 Caddy 反代自动证书，或 Cloudflare Tunnel），compose 仍只绑 `127.0.0.1`，不要把 8080 裸奔公网。自托管环境没有平台令牌：在 Zeta 登记时平台令牌填任意随机字符串占位即可，不需要在入口层再加一层校验，安全由应用令牌（`SPARK_API_TOKEN`）把关。

## 接口

### `GET /health`

```json
{"ok": true, "sparkVersion": "3.5.3"}
```

自愈：服务内置看门狗，每 30 秒探活 Spark 网关；若 JVM 已死而 Python 进程尚存（例如宿主机冻结/恢复后 Spark 心跳超时退出），连续两次探活失败即主动退出进程，由 Docker 重启策略（`restart: unless-stopped`）拉起新 JVM 自动恢复。会话状态在重启后清空，metastore 数据不受影响。

### `POST /query`

请求头 `X-Auth-Token: <你的令牌>`，请求体：

```json
{"sql": "select 1", "maxRows": 1000, "session": "alice"}
```

| 字段 | 说明 |
|---|---|
| `sql` | 单条 SQL（一次一句） |
| `maxRows` | 最多返回行数，硬上限 5000 |
| `session` | 可选。多会话标识（≤128 字符）：同一标识共享当前库、临时视图与会话配置，不同标识互相隔离；库表（metastore）全会话共享。不传则走默认会话 |

成功返回：

```json
{"columns": ["1"], "rows": [[1]], "rowCount": 1, "truncated": false}
```

错误形态：令牌缺失/错误 → 401 `{"error": "unauthorized"}`；请求体不合法 → 400；SQL 执行出错 → 200 + `{"error": "..."}`。

### `POST /cancel`

取消指定会话正在执行的查询。请求头与 `/query` 相同（`X-Auth-Token`），请求体：

```json
{"session": "alice", "execution": "550e8400-e29b-41d4-a716-446655440000"}
```

`execution` 为可选的执行 ID（字符串，≤128 字符）：提供时只取消该次执行的 job group（`session:execution`），迟到的取消不会误杀同会话的下一条执行；不提供时仍按会话级取消（老行为不变）。`/query` 同样接受可选的 `execution`，提供时按同一规则设 job group。`session` 缺省/为空时取消默认会话的在途查询（默认会话为 `default`）。实现方式：查询执行时按会话（或会话:执行）设置 Spark job group，`/cancel` 在另一线程调用 `cancelJobGroup` 杀掉该组在途的 job；会话本身与 SparkSession 不销毁，临时视图与当前库保留。无在途 job 时同样返回 `{"ok": true}`（幂等）。被取消的查询以 200 + `{"error": "… Job … cancelled part of cancelled job group …"}` 返回；鉴权与请求体规则同 `/query`（错令牌 401、非法请求体 400；未设置 `SPARK_API_TOKEN` 时一律 401）。

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

## 数据存在哪

数据目录（默认 `/data/spark`）内：

- `metastore_db/`：Derby 数据库，存库/表定义
- `warehouse/`：托管表数据，一库一子目录、一表一子目录，Parquet 格式
- `tmp/`：Spark 本地临时目录

把这个目录挂到持久卷上即可长期保留；删掉卷 = 库表全丢。临时视图与会话状态只在内存里，服务重启即清（库表不受影响）。

## 安全提示

- 令牌就是唯一的门：用随机长令牌（如 `openssl rand -hex 32`），不要提交进 git。
- 默认 compose 只绑 `127.0.0.1`。要对外提供服务，请走带鉴权的反向代理或隧道，不要把端口裸奔在公网——这个服务能执行任意 SQL，包括读写文件类语句。

## 本地构建

```bash
docker build -t spark-sql-service .
```

## 说明

本服务从 Zeta 项目的 SparkSQL 数据源后端实践中抽出：单机练习/轻量场景够用，不是集群方案——要 Standalone/YARN 集群、Kyuubi/JDBC 网关或多人生产隔离，这个薄服务都不适合。

## License

MIT
