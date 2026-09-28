# Flight Gateway 部署与配置说明

> 面向形态：standalone Spark 集群 + ZooKeeper 集群（ZK 引擎管理模式）。
> 决策依据见 `fg-design.md`（D22 引擎管理 / D24 cluster 加固 / D25 TLS / D26 指标 / D27 scroll 翻页）。
> 所有配置键的权威默认值见 `fg-common/src/main/resources/fg-reference.conf`。

## 0. 架构与端口总览

```
客户端(pyarrow/JDBC/ADBC)
   │ Flight SQL (gRPC, :32010)
   ▼
┌─ flight-gateway (×N) ──────────────┐
│  :32010 Flight    :9091 /metrics   │
│  依赖: PostgreSQL / ZK / MinIO(S3) │
└────────────┬───────────────────────┘
             │ SparkLauncher 提交 (需本机 SPARK_HOME/bin/spark-submit)
             ▼
   standalone master (spark://master:7077) ── workers 上拉起 FgEngineMain driver
             │ ZK 注册 (:2181)
             ▼
   引擎 Connect :15002 + admin HTTP :15003（每节点一组）
             │ 结果直写
             ▼
   MinIO/S3 (fg-results bucket)
```

| 端口 | 组件 | 说明 |
|---|---|---|
| 32010 | 网关 Flight | 客户端入口 |
| 9091 | 网关 metrics | Prometheus 抓取（`fg.metrics.*`） |
| 2181 | ZooKeeper | 引擎发现/锁 |
| 5432 | PostgreSQL | 操作/会话登记 |
| 9000 | MinIO | 结果对象 |
| 15002/15003 | 引擎 connect/admin | **cluster 模式固定端口**——同节点多引擎会冲突，真实集群引擎落不同节点即天然错开 |

## 1. 构建制品

```bash
cd flight-gateway
mvn -q package -DskipTests
```

| 制品 | 部署到 | 用途 |
|---|---|---|
| `fg-dist/target/fg-dist-0.1.0-SNAPSHOT.jar` | 网关主机 | 网关 fat jar（含全部依赖） |
| `fg-engine-spark/fg-spark-app/target/fg-spark-app-0.1.0-SNAPSHOT.jar` | 网关主机（可读路径） | 引擎 shaded jar（curator/zk/guava 已 relocate），提交时上传 |
| `fg-engine-spark/fg-spark-sink/target/fg-spark-sink-0.1.0-SNAPSHOT.jar` | 网关主机（可读路径） | 结果写 DSv2，经 `--jars` 分发到 executor |

JDBC 驱动用 fork 构建（fg-p1~p3 补丁）：`nonggialiang/arrow-java` 的 `fg-19.0.0-presign` 分支。

```bash
git clone -b fg-19.0.0-presign git@github.com:nonggialiang/arrow-java.git
cd arrow-java && mvn -pl flight/flight-sql-jdbc-core,flight/flight-sql-jdbc-driver install \
    -DskipTests -Dspotless.check.skip=true -Dcheckstyle.skip -Drat.skip=true
# 以 19.0.0-fg-p1 坐标进本地 m2；改了 fork 源码后注意 Maven 增量缓存偶发不重编，
# 症状是"改了没生效"——rm -rf 模块 target 强制全量
```

## 2. 前置依赖

**网关主机（每台）：**
- JDK 17
- 一个 Spark 发行目录（`$SPARK_HOME`）——**只需要 `bin/spark-submit` 可执行**，不需要连集群的完整配置；SparkLauncher 靠它 spawn 提交进程
- 网络可达 ZK / PG / MinIO / standalone master

**引擎节点（standalone workers）：**
- Spark 3.5.x 发行包（集群本身已有）
- **S3A 两件**：`hadoop-aws-3.3.4.jar` + `aws-java-sdk-bundle-1.12.262.jar`，二选一：
  - 预放进每个节点的 `$SPARK_HOME/jars/`（推荐，免每次分发）；或
  - 经 `launch.extra-jars` 携带（每次拉起随 --jars 分发，aws-sdk-bundle ~200MB 时不推荐）
- fg-spark-sink 同理：预放进 `$SPARK_HOME/jars/` 或经 `extra-jars` 携带
- 15002/15003 可绑定（引擎侧有 preflight 快败，起不来会反馈到网关拉起失败）

**基础设施：**

```sql
-- PostgreSQL：建库建用户即可，表由 Flyway 自动创建（V1）
CREATE DATABASE flightgateway;
CREATE USER fg WITH PASSWORD 'fg';
GRANT ALL PRIVILEGES ON DATABASE flightgateway TO fg;
```

MinIO：创建 bucket `fg-results`。ZK：无特殊要求，引擎 znode 自动创建（`/{ns}_v1_{SHARE}_spark/...`）。

## 3. 网关配置

推荐 `gateway.conf` 放 classpath 或用 `-Dfg.*` 逐项覆盖（含逗号的值勿走 `-D`，会被切成 list）。生产可用完整示例：

```hocon
fg {
  flight {
    port = 32010
    tls {                                   # 可选；不配则明文
      enabled = true
      cert-chain = "/etc/fg/tls/server.crt"   # PEM 双文件
      private-key = "/etc/fg/tls/server.key"
      # client-ca-cert = "/etc/fg/tls/ca.crt" # 可选 mTLS（设备级，与 Basic 正交）
      # KeyStore 模式二选一：key-store/key-store-password/key-store-alias/key-password
    }
  }

  db {
    url = "jdbc:postgresql://pg-host:5432/flightgateway"
    username = "fg"
    password = "fg"
    max.pool.size = 10
  }

  zk {
    addresses = "zk1:2181,zk2:2181,zk3:2181"   # ← 非空即启用引擎管理（核心开关）
    namespace = "flight-gateway-prod"
  }

  engine {
    share.level = "USER"                       # USER | GROUP | SERVER | CONNECTION
    share.subdomain = "default"
    # GROUP 模式才需要：
    # share.group-mapping = ["bi:teamA","etl:teamA"]
    # SERVER 模式才需要：
    # share.server-user = "shared"

    initialize.timeout = 300s
    idle.timeout = 30m
    launch.max.concurrent = 5
    open.max-attempts = 3
    open.retry-wait = 2s

    spark {
      connect.uri = "sc://localhost:15002"     # ZK 模式下不使用，保留默认即可

      launch {
        # ── 集群提交（standalone）──
        master = "spark://spark-master-host:7077"
        deploy-mode = "cluster"
        home = "/opt/spark"                    # 网关本机 SPARK_HOME（spark-submit 所在）
        app-jar = "/opt/fg/lib/fg-spark-app-0.1.0-SNAPSHOT.jar"
        extra-jars = []                        # sink/S3A 预装了就不用带
        driver-memory = "2g"

        # ── 公共基础 spark conf ──
        conf-file = "/opt/fg/conf/engine-base.conf"   # spark-defaults.conf 风格 properties
        conf { }                                # 少量覆盖项（优先级高于文件）

        # ── 引擎固定端口（每节点一组）──
        cluster.connect-port = 15002
        cluster.admin-port = 15003

        log-dir = "/var/log/fg/engines"        # submitter stdout；driver 日志在 worker work dir
      }
    }
  }

  result {
    bucket = "fg-results"
    prefix = "results"
    s3 {
      endpoint = "http://minio-host:9000"      # 真 S3 填 https
      access-key = "..."
      secret-key = "..."
    }
    retention = 24h
    endpoint.mode = "relay"                    # 默认 relay；https 需客户端协商
  }

  auth.basic.users = ["alice:pass1", "bob:pass2"]   # 多用户；空则回落 username/password 单键对

  metrics {
    enabled = true
    port = 9091
  }
}
```

**engine-base.conf**（`launch.conf-file`，Properties 语义：`#`/`!` 注释、`=`/`:`/空白分隔、反斜杠行续、`~` 展开；含逗号的值天然安全）：

```properties
# /opt/fg/conf/engine-base.conf
spark.sql.adaptive.enabled=true
spark.sql.shuffle.partitions=200
spark.eventLog.enabled=true
spark.eventLog.dir=/opt/spark/events
```

**引擎 conf 优先级**（从低到高）：

```
$SPARK_HOME/conf/spark-defaults.conf（机器级，spark-submit 原生加载）
  < fg.engine.spark.launch.conf-file（FG 公共基础）
    < fg.engine.spark.launch.conf 对象（撞键覆盖文件）
      < FG 内建 payload（spark.fg.* 保留域：zk 发现/端口/时限/s3 凭证）
```

cluster 模式 JDK17 `--add-opens` 系列由 FG 自动合并进 `spark.driver.extraJavaOptions`（与你的值叠加），不用手配。

## 4. 启动

```bash
java -Xmx2g \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  -jar /opt/fg/lib/fg-dist-0.1.0-SNAPSHOT.jar
```

systemd 示例：

```ini
[Unit]
Description=Flight Gateway
After=network.target postgresql.service

[Service]
User=fg
ExecStart=/usr/bin/java -Xmx2g --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  -jar /opt/fg/lib/fg-dist-0.1.0-SNAPSHOT.jar
Restart=on-failure
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

**多实例**：每台同配置即可（无本地状态，PG/ZK/S3 全局共享）；LB 只需支持 **HTTP/2 长连接且 idle timeout > gRPC keepalive**（JDBC 连接天然 sticky——一条连接一个 HTTP/2，LB 只在建连分配一次）。断线重连可能落他实例，正确性不受影响。

## 5. 部署验证

```bash
# ① 网关起活 + Flyway 建表
journalctl -u flight-gateway | grep "Flight Gateway started"

# ② 指标面
curl -s http://gw-host:9091/metrics | grep -E "^fg_engine|^jvm_memory_used" | head

# ③ 首查冷启动（任一客户端触发一次查询）
#    standalone master UI 出现 fg_USER_<user> 应用 → RUNNING
{zkCli 或 zk-shell} ls /flight-gateway-prod_v1_USER_spark/<user>/default
#    指标走动：fg_engine_launches_total 1、fg_engine_live 1

# ④ 复用（第二次查询不重拉，znode 数不变）+ 查询结果正确

# ⑤ kill 引擎进程 → 下一次查询自动重拉（恢复链路），旧会话 engine_lost sticky
```

JDBC 连串（fork 驱动）：

```
jdbc:arrow-flight://gw-host:32010?useEncryption=true&user=alice&password=pass1
# 明文部署时 useEncryption=false
# TYPE_SCROLL_INSENSITIVE statement 自动走服务端分页随机翻页（D27）
```

## 6. 运维要点

| 项 | 说明 |
|---|---|
| 引擎生命周期 | idle 30m 自杀、可选 `max-lifetime`、CONNECTION 随 CloseSession 下线（admin `/engine/stop`）——全自动，无需运维介入 |
| 日志 | submitter stdout 在 `launch.log-dir/{refId}.log`；cluster 模式 driver 的 stdout/stderr 在 **worker 的 `work/driver-*/`**（spark.worker.ui 可跳转） |
| 结果清理 | `fg.result.retention` 过期策略；purge 按 manifest 清单连带 `.bidx` 边车 |
| 关键指标 | `fg_engine_{live,launches_total,launch_failures_total,recoveries_total,evictions_total}`、`fg_query_{registered,outcome}_total`、`fg_relay_{streams,failures}_total`、`fg_scroll_pages_total`、`fg_session_closed_total{reason}`（**engine_lost 突增 = 引擎不稳**） |
| 恢复语义 | 恢复只挂 sessionStatus（connect-refused → 守卫注销 → 重拉），submit/attach 绝不自动重试（防重复执行） |

**已知边界**：

1. **同节点引擎撞固定端口 15002/15003**：USER/GROUP 天然按用户分散；`share.level=CONNECTION` 时同节点并发会话会撞——启用 CONNECTION 前确认引擎异节点分布。
2. **拉起超时的卡死引擎**：网关无进程句柄，靠引擎侧 idle 看门狗 / max-lifetime 自愈。
3. **conf 值含逗号**：走 conf-file / gateway.conf，勿走 `-D`（会被切 list）。
4. **`spark.fg.*` 前缀保留**：网关运行时下发域，勿在 conf-file/launch.conf 里覆盖。
