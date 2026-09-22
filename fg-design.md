# Flight Gateway 产品设计文档

> 版本 v0.16（relay 多 endpoint） · 命名代号 **FG（Flight Gateway）**
>
> v0.15 → v0.16 变更：**relay 模式支持多 endpoint**——ticket 信封扩展 kind：`STREAM`（查询级，GetFlightInfo 快返兼容路径用，完成前 part 数未知）与 `PART`（分片级，最终 poll 按 manifest 铸 N 张票，DoGet 只流指定 part）——旧客户端获得与 https 相同的并行取数与单分片重试能力。**H4 跨模式统一**：顺序敏感查询两种模式都返回单 endpoint（spec"clients may ignore ordered"）；relay 池容量按 并发查询×part 数 重估；新增 `fg.result.relay.single-stream` 保险配置（防只 DoGet 首个 endpoint 的非合规客户端，PoC ⑥ 验证）。
>
> v0.14 → v0.15 变更：**取消与 attach 解耦**——CancelFlightInfo 服务实例（B）不 attach、直接 `Interrupt(operationId)`（不要求 attach，A 挂着不影响）+ CAS 条件 UPDATE 行迁移；attach 者（A）被动感知流终止后撞 CAS 空转、只做 ReleaseExecution/本地清理；part 清理由引擎 BatchWrite.abort 自动完成。行迁移统一 CAS（WHERE status=RUNNING）使完成/取消双写者互斥。
>
> v0.13 → v0.14 变更：**reattachable execution 升级为正常主路径**——触发实例提交后捕获 operationId 落行即 **detach**（执行期完全无状态）；后续 poll 服务实例（任意一台）经 **DB attach 租约仲裁**后 `Reattach(operationId)` 挂执行流获取完成信号，终态后 `ReleaseExecution`。断流恢复不再是特殊路径；gateway 执行期滚动重启零影响；完成信号分层 = attach 流结束（快）> 行推进 > manifest 对账（兜底，attach 不承载正确性）。
>
> v0.12 → v0.13 变更：**改用 Flight v25 标准原语，废弃全部自定义取数 action**——① 主链路改 **PollFlightInfo**（首次=注册+触发执行+快返 PollInfo；后续=长等待，完成时一次性返回全部 endpoints），**FgGetEndpoints 退役**（续期由标准 RenewFlightEndpoint 接管）；② 取消改用标准 **CancelFlightInfo**（携带 FlightInfo，首次 PollInfo 的 info 即取消凭证）；③ **https Location 按 v25 规范作为一等 endpoint 表示**（ticket 置空、HTTP GET、Arrow IPC 假定、presigned URL 规范认可）；④ 引入 **DB 操作表（OperationStore）**：执行状态/operationId/mode 落库，跨实例 poll/cancel 天然可服务——**MinIO notification 基建退役，ZK 回归纯引擎发现**；⑤ 触发时机后移：首个 poll/GetFlightInfo 注册并触发执行（lazy）；⑥ ORDER BY 按 spec"顺序敏感应单 endpoint"收紧为 https 模式强制 coalesce(1)。
>
> v0.11 → v0.12 变更（设计评审三连修）：**H1** GetFlightInfo 改为**快速返回**（仅 AnalyzePlan 取 schema，执行异步）——Flight SQL 的 CancelQuery 携带 endpoint.ticket，同步阻塞到 COMPLETED 会让客户端在执行期间无 ticket 可取消；ticket **提交时即签发**，DoGet 挂 future 等待，新增 `FgGetEndpoints` action 供智能客户端完成后取 presigned URL（D2 随之消解）；补 **queryId↔operationId 映射**（Connect Interrupt/Reattach 的目标是 operationId，首响应捕获）。**H4** ORDER BY 顺序保证：顶层全局排序时禁用 `repartition(K)`（round-robin 摧毁序），自然分区依赖 RangePartitioner 分区间有序 + manifest 按 index 排序。**H5** ticket 信封补字段：完整 per-query `resultKeyPrefix`（含 queryId）+ `kind`，manifest 路径可直推；ticket 定为**查询级**（非 per-part）。
>
> v0.10 → v0.11 变更：① **§4.4 结构重排——路线 B（Connect 薄壳）成为主叙事**：4.4.1=薄启动壳、4.4.3=Connect client（plan 组装/进度/断流恢复），路线 A（自有协议）降级为 4.4.4 回退路线；§2.2 主链路同步按 B 改写；② **PoC 重分类**：① WriteOperation 等价性预验证通过（官方 Scala 客户端 `spark.sql().write.format().save()` 即生成该 plan），退出风险清单；新增硬性 ⑤ **断流恢复**（reattachable execution + manifest 对账），PoC 精力集中于 ③⑤；③ §4.8 缓存扩围到 listDatabases/listTables。
>
> v0.9 → v0.10 变更：**移除对象注册表**（内存+ZK 的 queryId→对象映射）——MinIO 里的 `manifest.json` 即注册表（确定性路径、字段全量），**ticket 改为 HMAC 签名的自描述信封**（bucket/prefix/user），DoGet/re-presign 无状态、gateway 重启免疫；新增 D13 风险（签名/轮换/每次 manifest GET 的代价）。
>
> v0.8 → v0.9 变更：① **移除 maxBytes 熔断**（spec 去 maxBytes 字段，超大结果靠 MinIO 单对象上限 + retention 治理，护栏只留查询超时）；② 引擎 **deploy-mode 改为 cluster**（driver 运行于集群，launcher 提交后退出，就绪判定仅依赖 ZK 注册）；③ G3 度量改为诚实表述：智能客户端字节不过 gateway / 标准客户端经中继（受有界池与背压约束）。
>
> v0.7 → v0.8 变更：新增 §4.4.4 **基于 Spark Connect 的薄 Kit 备选路线**（SQL 包装物化：CTAS `USING fg-result` 触发 executor 直写，复用 Connect 原生 ExecutePlan/Interrupt/Config/**Catalog 服务**，引擎侧自定义缩为 DSv2 jar + 薄启动壳）；列为 v1 首选实现，自有控制协议降级为回退路线；M1 增加三个 PoC 验证点。SPI 不变——两条路线是 Kit 内部实现差异，对外零感知。
>
> v0.6 → v0.7 变更：**MaterializationSpec 移除 Credential 字段**——v1 凭证不经 spec 下发，改为引擎启动时注入静态 scoped key（引擎级配置，限定结果前缀只写）；SPI/proto 预留扩展位（字段号保留 + 未知字段忽略），未来支持每查询 presigned-PUT/STS 时只加字段、不破坏契约。
>
> v0.5 → v0.6 变更：**元数据目录族命令升为 v1 必做**（支撑 Arrow Flight SQL JDBC 驱动）：SqlInfo/Catalogs/DbSchemas/Tables/TableTypes/XdbcTypeInfo 完整实现（映射引擎 catalog，JDBC pattern 过滤）；主外键系列返回空结果集；新增 §4.8 实现方案与 Catalog 控制面 RPC；里程碑 M2 承接，验收加入 JDBC DatabaseMetaData。
>
> 命名变更：代号由 SFG（Spark Flight Gateway）更名 **FG（Flight Gateway）**——网关定位引擎中立，Spark 只是首个引擎 Kit；前缀/命名空间/注册名同步从 `sfg` 改为 `fg`（ZK 命名空间默认 `flight-gateway`）。
>
> v0.3 → v0.4 变更：**移除 S3 Compose，改为 `fg.result.partitions` 分区数配置**——未设置=自然分区多 part；=1=`coalesce(1)` 单对象（顺序 IPC stream）；=K=`repartition(K)` 精确 K part。理由：Arrow IPC stream 不能字节级拼接（首个 EOS 终止读取），服务端 Compose 需帧级手术、实现脆；写入端直接决定输出形态最干净。
>
> v0.2 → v0.3 变更：Spark sink 升级为 **DataSource V2 自定义写入**——executor task 并行直写 part 文件，part 元数据经 `WriterCommitMessage` → `BatchWrite.commit` 聚合（对标 Iceberg `DataFile`/`SnapshotSummary`/abort 清理）；driver 彻底退出数据路径；**增量迭代器不再需要**（仅保留给未来 inline 小结果模式）。
>
> v0.1 → v0.2 变更：结果物化从"gateway 拉流写 MinIO"改为"**引擎端直接写 MinIO**"；gateway 退化为纯控制面；引擎适配从单一 adapter 变为 **Engine Kit（sink 实现 + 引擎 app + gateway 侧 client）** 三件套，机制对标 Kyuubi `saveToFile`（引擎端 `DataFrame.write` 思路）+ Kyuubi 引擎生命周期管理。

---

## 1. 概述

### 1.1 背景与目标

以 Spark 为首个引擎的**引擎中立 Flight SQL 网关**：Flight SQL 客户端（ADBC、pyarrow）提交 SQL 后，gateway 编排后端引擎执行，**引擎端将结果以 Arrow IPC stream 直接物化到 MinIO**，gateway 对客户端返回 presigned URL（或中继）。

**核心目标**

| # | 目标 | 度量 |
|---|---|---|
| G1 | 标准 Flight SQL 兼容 | ADBC 驱动可连通、可查询、可取数 |
| G2 | 引擎多路复用 | 同一引擎服务多用户多会话（share level 可配） |
| G3 | gateway 不承载数据主体 | **智能客户端：结果字节不经过 gateway**（直连 MinIO presigned URL）；标准客户端：经 relay 中继，受有界池 + 背压 + O(1 batch) 内存约束；gateway 宕机不影响已物化结果的获取 |
| G4 | 引擎可替换 | 新引擎以 **Engine Kit**（sink + app + client）接入，协议层零改动 |
| G5 | 生产级服务骨架 | 生命周期、线程、内存、metrics、配置按 Dremio 服务框架范式 |

### 1.2 非目标（v1 不做）

- 不做写路径（DoPut 入库）、DML
- 不做跨引擎联邦查询
- 不做结果集谓词下推到 MinIO（只顺序回放）
- 主外键等关系型元数据返回**空结果集**（引擎 catalog 无此语义，Iceberg 表可后续增强）

### 1.3 术语

| 术语 | 含义 |
|---|---|
| **Engine Kit** | 一种引擎的完整适配三件套：`sink`（引擎进程内的结果写入实现）+ `app`（引擎应用，负责封装用户 SQL 并挂上 sink）+ `client`（gateway 侧控制面客户端） |
| Engine | 集群上运行的一个引擎 app 实例（如一个 Spark 应用） |
| engineSpace | 引擎在 ZK 上的发现命名空间（Kyuubi 概念） |
| MaterializationSpec | gateway 下发给引擎的物化指令（对象路径、格式参数、batch hint） |
| Result Object | 一次查询结果对应的 MinIO Arrow IPC stream 对象 |
| Relay | gateway 从 MinIO 流式转发给客户端的 DoGet 兼容模式 |
| OperationStore | DB 作业表：queryId→执行状态/operationId/mode（跨实例共享，量级=在途查询数） |

---

## 2. 总体架构

```
                ┌──────────────────────────────────────────────────┐
 Flight SQL     │         Flight Gateway（控制面 + 标准客户端中继）   │
 客户端         │                                                  │
 (ADBC/pyarrow) │  FlightSql Frontend ── QueryOrchestrator         │
    │           │        │(DoGet relay)        │ SqlEngine(SPI)    │
    │  gRPC/TLS │        │                  SparkEngineClient ─────┼─Spark Connect▶ Engine App
    ├──────────▶│  ResultRelay             (Kit 的 client 件)      │              │
    │           │  ResultStore(presign/retention)                  │              ▼
    │           └──────────┬──────────────────────┬───────────────┘  df.write.format("fg-result")
    │                      │                      │                     │ executor task 直写 part.arrows
    │  presigned GET ◀─────┘                      │ ZK: 发现+锁+注册    │ commit msg 聚合 → manifest
    ▼                                             ▼                    ▼
  MinIO ◀────────── 结果直写(executor) ───────────────── Spark Engine App（standalone 集群）
```

**数据面与控制面彻底分离**：

- 控制面（gateway ↔ engine）：SQL 文本、MaterializationSpec、状态事件——KB 级；执行状态另持久化于 **DB 作业表**（跨实例共享）
- 数据面（engine → MinIO）：Arrow batch 流，不经过 gateway
- 取数面（client ← MinIO/gateway relay）：presigned GET 或 DoGet 中继

### 2.1 Maven 模块划分

```
flight-gateway/
├── fg-common               配置、线程池、allocator、metrics（Dremio 骨架）
├── fg-frontend            Flight SQL producer、会话管理、DoGet relay
├── fg-orchestrator        查询状态机、超时/取消、物化指令生成
├── fg-engine-spi          SPI 契约：SqlEngine/EngineCatalog/EngineSession 接口 + MaterializationSpec/Result + 控制协议 proto（只定义，不含实现）
├── fg-result-store        签名 ticket、presign、manifest 校验、retention（无写入路径、无结果状态）
├── fg-engine-spark        【Engine Kit for Spark】
│   ├── fg-spark-sink      FgResultDataSourceV2（引擎进程内：DSv2 写 + commit 元数据聚合）
│   ├── fg-spark-app       薄启动壳：SparkConnectServer main + ZK agent + idle/max lifetime（+fg-result jar）
│   └── fg-spark-client    gateway 侧 SqlEngine 实现（Connect 客户端/薄 stub：plan 组装 + 断流恢复）
└── fg-dist                打包、启动入口（GatewayDaemon）
```

### 2.2 端到端流程（主链路）

```
1. 客户端 Handshake/BasicAuth → gateway 建会话（引擎 session 延迟创建）
2. PollFlightInfo(descriptor{sql}) ① —— 注册 + 触发（快返）：
     ├─ OperationStore(DB) 幂等 INSERT：queryId/sql/user/resultKeyPrefix/mode/schema 占位
     │    （唯一键 = 会话+SQL 指纹，网络重试不重复执行）
     ├─ ensureEngine（§5）→ client（Connect）ExecutePlan[WriteOperation + CollectMetrics]（异步触发）
     │    ├─ Connect server: SQL→DF→列名去重→分区控制（§4.4.3，ORDER BY 保护）→ V2 写
     │    │    ├─ executor task: Arrow 攒批 → part-{p}-{a}.arrows 直写 MinIO
     │    │    │    writer.commit() → PartMetadata（= WriterCommitMessage）
     │    │    ├─ driver commit(msgs): 聚合 → manifest（对标 Iceberg SnapshotSummary）
     │    │    └─ abort(msgs): 按清单删 part（对标 Iceberg abort）
     │    ├─ 首响应捕获 operationId → UPDATE 行（Cancel/后续 attach 用）→ **立即 detach**（断开流；
     │    │    引擎照常执行、manifest 照落——reattachable 主路径，触发实例自此对本查询无状态）
     │    └─ AnalyzePlan schema、ObservedMetrics 消费移交后续 attach 者
     ├─ AnalyzePlan 取 result schema（有界 fg.query.prepare.timeout）→ UPDATE 行
     └─ 快返 PollInfo{ info: FlightInfo{schema, endpoints=[], app_metadata{queryId}},   ← info 即取消凭证
                       flight_descriptor: {poll=queryId}, progress: 不设 }
3. PollFlightInfo(poll descriptor) ②… —— 长等待（规范："结果变化前不响应"）：
     ├─ 行 RUNNING → 抢 attach 租约（行内 attach_owner 条件 UPDATE，租约过期可接管）：
     │    ├─ 赢家：Reattach(operationId) 挂执行流（服务端 offset 续传，含 ObservedMetrics）
     │    │    → 流结束即知完成 → UPDATE 行 COMPLETED → ReleaseExecution 释放引擎缓冲
     │    └─ 输家：有界间隔查行（fg.poll.db.interval）等赢家推进
     ├─ 单次等待上限 fg.poll.max-wait，到点返回未完成 PollInfo，客户端续 poll——防中间设备掐长 RPC
     ├─ 行 COMPLETED → 读 manifest → 按 mode 构造 endpoints → 一次性全部返回：
     │    PollInfo{ info: FlightInfo{schema, ordered, totalRecords/Bytes, endpoints},
     │              flight_descriptor: unset }
     └─ 对账兜底：等待超时且行仍 RUNNING → 查 manifest 存在性（触发实例死亡的修复路径）
4. 取数（按 mode 二选一，不可混发——endpoints 是分片拼接语义，混发=数据翻倍）：
     ├─ https 模式：N × endpoint{location=presigned URL, ticket 空, expiration_time=TTL}
     │    （v25 标准：客户端 HTTP GET 直连 MinIO，Arrow IPC 假定；ORDER BY 查询按 spec 返回
     │     单 endpoint → 写端强制 coalesce(1)，见 H4/D11）
     └─ relay 模式（旧客户端兼容）：最终 poll 返回 N × endpoint{ticket=PART 票(partIndex)}
          （并行 DoGet 各分片、单分片可重试，与 https 模式对齐；顺序敏感查询返回单 STREAM 票）；
          DoGet 验签 → 读 manifest 定位 part → 流式该 part（O(1 batch) + 背压）；保险配置
          fg.result.relay.single-stream 可切回单 STREAM 票（防只 DoGet 首个 endpoint 的客户端）
5. 旧客户端入口：GetFlightInfo = 同 ② 注册+触发+快返 FlightInfo{STREAM 票 endpoint}（完成前 part 数未知，只能查询级票）
6. CancelFlightInfo(info)——"B 下杀手，A 收尸"（取消与 attach 解耦）：
     实例 B：读行（已终态→NOT_CANCELLABLE）→ Interrupt(operationId)（不要求 attach，
       陪伴者 A 挂着不影响）→ CAS 条件 UPDATE 行=CANCELLED（WHERE status=RUNNING；
       0 行命中=A 已迁 COMPLETED→重读返回 NOT_CANCELLABLE）→ 返回 CancelStatus
     实例 A（attach 者）：流因取消终止 → 行迁移撞 CAS 空转 → ReleaseExecution + 本地清理
     引擎：Interrupt → 执行取消 → BatchWrite.abort 自动按清单删 part（善后无网关参与）
     引擎不可达 → CANCEL_STATUS_UNSPECIFIED（规范：客户端可重试）；重复取消幂等
7. 续期：RenewFlightEndpoint（标准 action）——presign 过期后延期
```

---

## 3. 功能需求与验收标准

### F1 Flight SQL 协议面

| 需求 | 说明 |
|---|---|
| 认证 | Basic token；TLS 默认开启 |
| 命令/RPC | **PollFlightInfo**（主链路：注册+触发+轮询）、**GetFlightInfo**（旧客户端兼容路径，同语义快返）、**CancelFlightInfo**（标准取消，携 FlightInfo）、**RenewFlightEndpoint**（标准续期，presign 延期）、`CommandStatementQuery`、`CreatePreparedStatement/ClosePreparedStatement`（v1.1 含 DoPut 参数绑定）、`CommandGetSqlInfo`、session actions |
| 元数据 | **v1 完整实现目录族**（Flight SQL JDBC 兼容）：SqlInfo / Catalogs / DbSchemas / Tables / TableTypes / XdbcTypeInfo 映射引擎 catalog，支持 JDBC pattern（`%`/`_`）过滤与分页 ticket；主外键/CrossReference 返回空结果集（见 §4.8） |
| 会话 | header/cookie 关联（仿 Dremio `ServerCookieMiddleware`），gateway 会话 ↔ 引擎会话一一映射 |

**验收**：pyarrow `flight_sql_client` 与 ADBC 完成 open→query→fetch→close；**Arrow Flight SQL JDBC 驱动**可连接、执行 SQL、`DatabaseMetaData.getSchemas/getTables/getColumns` 正常返回。

### F2 查询执行编排（PollFlightInfo 模型）

- **首个 PollFlightInfo = 注册 + 触发**（lazy：谁要结果谁触发）：OperationStore 幂等 INSERT（唯一键=会话+SQL 指纹）→ 异步提交引擎 → 快返 PollInfo（`info` 即取消凭证，app_metadata 含 queryId）
- **后续 poll = 长等待**：行 COMPLETED 后一次性返回全部 endpoints（`flight_descriptor: unset`）；等待有上限（`fg.poll.max-wait`），到点返回未完成、客户端续 poll
- 行状态机 `RUNNING → {COMPLETED | FAILED | CANCELLED}`（RECONCILING 为 attach 者内部短暂态，不落行）；迁移写者 = attach 租约持有者（完成/失败）或取消服务实例（CANCELLED），**CAS 互斥**；跨实例靠查行 + manifest 对账兜底
- 取消：`CancelFlightInfo(info)` → 行解 queryId/operationId → `Interrupt`（**不要求 attach**）→ CAS 行迁移；完成/取消双写者经 CAS 互斥；返回标准 CancelStatus（不可达=UNSPECIFIED 可重试）
- progress 字段：**不设**（总行数未知，不编造 [0,1]）

### F3 结果物化（引擎直写）

- 引擎端以 **Arrow IPC stream format** 写 MinIO：每个 executor task 产出一个 part 文件（`Schema 帧 + N × RecordBatch + EOS`），driver 聚合 `manifest.json`（对标 Iceberg SnapshotSummary）
- 路径规范：`s3://{bucket}/{prefix}/{engineType}/{user}/{queryId}/part-{p}-{attempt}.arrows` + `manifest.json`
- **part 数由 `fg.result.partitions` 控制**：未设置 → 不引入 repartition，按计划自然分区写（写并行度最大，N 个 endpoint）；=1 → `coalesce(1)` 单 part 单对象单 endpoint（标准顺序 IPC stream，无额外 shuffle）；=K → `repartition(K)` 精确 K part / K endpoint
- 最终 FlightInfo endpoints **按 mode 二选一**（注册时协商落行，不可混发）：**https 模式** = N × `{location=presigned URL, ticket 空, expiration_time=TTL}`（v25 标准，HTTP GET）；**relay 模式** = N × `{ticket=PART 票}`（分片并行 DoGet）或顺序敏感时 1 × `{ticket=STREAM 票}`。**ORDER BY 跨模式统一单 endpoint**（spec"clients may ignore ordered"）：https 强制写端 coalesce(1)，relay 返回单 STREAM 票
- 写失败 → `BatchWrite.abort(messages)` 按精确清单删 part（对标 Iceberg abort），上报 FAILED，retention 兜底清扫

### F4 引擎抽象（Engine Kit 模式）

见 §4.3。验收：**协议层与编排层零引擎概念；新引擎 = 新增一个 Kit 模块**。

### F5 引擎生命周期管理（Kyuubi 式）

见 §5。验收：同用户并发首查单引擎；引擎死亡 znode 自动消失、自动拉新；gateway 多实例不冲突。

### F6 服务框架（Dremio 范式）

见 §6。

---

## 4. 实现方案

### 4.1 Flight Sql 前端层（gateway）

**照搬 Dremio 的类**（`services/arrow-flight` 摘出改造）：

| Dremio 类 | 用途 |
|---|---|
| `DremioFlightServer` | builder + TLS + keepalive + executor 装配（改 FgFlightServer） |
| `DremioBackpressureStrategy` | relay 路径 `isReady/onReady` 等待策略，原样复用 |
| `RunQueryResponseHandler` 双 handler 模式 | 元数据流 Basic / relay 数据流 BackpressureHandling |
| `ServerCookieMiddleware` + 会话管理器 | 会话与 header 关联 |

**线程模型**：

| 线程池 | 类型 | 用途 |
|---|---|---|
| `flight-server-executor` | cached | producer 回调（短活；长等待由 orchestrator future 承担） |
| `relay-executor` | **有界**（默认 2×并发上限） | DoGet 中继 + 背压等待（唯一允许 `wait()` 的地方） |
| `engine-control-executor` | cached | 控制面 gRPC 事件流回调（轻量：状态/进度事件） |
| `housekeeping` | scheduled(2) | 引擎存活探测、idle 回收、retention 清扫 |

### 4.2 查询编排层（Orchestrator + OperationStore）

**执行状态落 DB（跨实例共享、重启存活），结果寻址仍以 manifest 为准（gateway 零结果数据状态）**：

```sql
-- fg_operation（作业表；量级 = 在途查询数，retention 清扫）
query_id          PK
fingerprint       UNIQUE(session_ref, sql_hash)   -- 首 poll 幂等（网络重试不重复执行）
user, session_ref, sql_text
result_key_prefix                                 -- {prefix}/{engineType}/{user}/{queryId}
mode              ENUM(HTTPS, RELAY)              -- 注册时协商（header/session option）
schema_bytes                                      -- AnalyzePlan 后回填
status            ENUM(RUNNING, COMPLETED, FAILED, CANCELLED)
connect_operation_id                              -- 触发实例首响应后回填，然后即 detach
attach_owner, attach_lease_until                  -- attach 租约（条件 UPDATE 仲裁，过期可接管）
engine_ref, error, terminal_at, created_at, updated_at
```

```java
// attach 者的内存态（不跨实例、不持久——持久事实在行与 manifest；触发实例 detach 后无任何状态）
public final class QueryExecution {          // 仅存在于当前 attach 租约持有者
  // queryId, CompletableFuture<MaterializeResult> future   // Reattach 流结束时 complete
  // volatile long rows;                     // ObservedMetrics（metrics/trace 用，不进 progress 字段）
  // 状态迁移：行 UPDATE + 内存 future complete 双写；终态后 ReleaseExecution
}
```

- **职责分工**：行迁移的写者有二且经 **CAS（WHERE status=RUNNING）互斥**——attach 租约持有者（流终自然的完成/失败迁移）与取消服务实例（Interrupt 后的 CANCELLED 迁移）；触发实例只负责注册+提交+落 operationId；其他实例读行服务 poll/cancel；行滞后时以 manifest 对账兜底（等待超时 → 查 manifest → 存在则自助作答并修复行）
- 所有状态迁移发 metrics + trace span（queryId 为 traceId，贯通 gateway 与引擎两侧）

### 4.3 引擎 SPI 与 Engine Kit

**gateway 侧接口**（控制面语义）：

```java
public interface SqlEngine extends AutoCloseable {
  String type();                                    // "spark", "flink"...
  EngineSession openSession(GatewaySession ctx);    // 背靠引擎 app 的会话
  MaterializeResult execute(EngineSession s, String sql, MaterializationSpec spec);
  void cancel(QueryId id);
  EngineCatalog catalog(EngineSession s);           // v1（JDBC 元数据目录）
}

/** 物化指令：gateway 只描述"写到哪、什么格式"，不知道怎么写 */
record MaterializationSpec(
  String objectUri,            // s3://bucket/prefix/{user}/{queryId}/
  boolean zstdCompression,
  int maxRecordsPerBatchHint) {}

record MaterializeResult(Schema schema, long rowCount, long bytesWritten, Duration executionTime) {}

// 凭证 v1 不在 spec 内：引擎用启动时注入的静态 scoped key 访问 MinIO（引擎级配置，见 D5）
// 扩展预留：控制协议 proto 保留字段号；未来增加 credential 字段（每查询 presigned-PUT/STS）时，
// 按"未知字段忽略"向前兼容——只加字段，不破坏 SqlEngine SPI 与既有引擎 Kit
```

**引擎侧 sink 契约**（跑在引擎进程内，Kit 的 `sink` 件）——**不定义跨引擎 Java 接口**，只约定 IO 契约，各引擎用原生写入框架实现（Spark 用 DataSource V2，见 §4.4.2；Flink 用其 Sink API）：

```
输入:  MaterializationSpec（objectUri 前缀、格式参数、batch hint；凭证走引擎级配置，不在 spec 内）
输出:  N × PartMetadata{uri, recordCount, bytes, order} + 聚合 manifest
语义:  全部 part 成功 → commit（元数据聚合上传）；任一失败/取消 → abort 按清单清理
格式:  每 part = Arrow IPC stream（Schema 帧 + batches + EOS）
```

**Kit 三件套职责**：

| 件 | 进程 | 职责 |
|---|---|---|
| `sink` | 引擎 | 该引擎的结果格式化与对象写入（Spark: DSv2 writer 直写 part + commit 元数据聚合） |
| `app` | 引擎 | 薄启动壳：fg-result DSv2 加载 + vanilla SparkConnectServer + ZK agent/idle 自杀（回退路线 A 时加厚为自有 ControlPlane gRPC，见 §4.4.4） |
| `client` | gateway | 实现 `SqlEngine`（含 **`EngineCatalog`**——路线 B 经 Connect Catalog 服务适配）；对编排层屏蔽引擎细节 |

**契约与实现的归属**：`SqlEngine`/`EngineCatalog`/`EngineSession` 等接口定义在 `fg-engine-spi`（契约层，gateway 与所有 Kit 的公共合同）；实现分布在 Kit 内——`client` 件做 gateway 侧适配（Connect 协议），`app`/`sink` 件提供引擎侧承载。协议层/编排层只 import SPI，不 import 任何 Kit。

**边界原则**：gateway 只认识 `sql + MaterializationSpec`；"怎么执行、怎么写、元数据从哪来"全部在 Kit 里。换引擎 = 新增 Kit，协议层/编排层零改动。

### 4.4 Spark Engine Kit

#### 4.4.1 引擎侧：薄启动壳（路线 B，v1 首选）

引擎 = 一个 Spark 应用（standalone，**deploy-mode=cluster**：driver 运行于集群节点，SparkLauncher 提交后即退出[waitEngineCompletion=false]，就绪判定**仅依赖 ZK 注册**getEngineByRefId，不依赖 driver 进程/日志）。

自定义部分只有一个薄启动壳（一个 main 类）：

```
main:
  1. 加载 fg-result DSv2（同进程注册，即 §4.4.2 的 sink 件）
  2. 启动 vanilla SparkConnectServer（Spark 官方 connect 服务入口）
  3. ZK agent: 向 engineSpace 注册 EPHEMERAL 节点（host:connectPort, refId, version）
     idle timeout / max lifetime / ZK 失联宽限自杀
```

- 执行、会话、取消、健康、元数据全部由 Connect server **原生承载**（ExecutePlan / Interrupt / Config / Catalog 服务）——本路线引擎侧无自有控制协议
- 引擎镜像 = Spark 官方发行版 + `fg-result` jar + 薄壳 jar，仅此而已

#### 4.4.2 sink（FgResultDataSourceV2：DSv2 写 + Iceberg 式 commit 元数据）

自定义 DataSource V2（注册名 `fg-result`），结构对标 Iceberg `SparkWrite`：

```
FgResultTable(SupportsWrite) ──newWriteBuilder──▶ FgWriteBuilder
   └─ build() → FgBatchWrite(BatchWrite)
        ├─ createWriterFactory() → 每 task 一个 FgArrowPartWriter(DataWriter<InternalRow>)
        │     行 → Arrow 向量攒批（batch hint 控制）→ part-{p}-{attempt}.arrows（IPC stream）
        │     writer.commit() → PartMetadata{uri, format, recordCount, bytes}   // = WriterCommitMessage
        │     writer.abort()  → 删本 task 半成品
        ├─ commit(WriterCommitMessage[] msgs)   // driver：全部 task 成功后
        │     聚合 PartMetadata → manifest.json 上传（对标 Iceberg SnapshotSummary）
        │     manifest 记录 part 顺序（= partition index）；partitions=1 即单对象
        │     → emit COMPLETED 控制事件
        └─ abort(WriterCommitMessage[] msgs)    // driver：按清单删已写 part（对标 Iceberg abort）
```

- part 顺序 = partition index（manifest 记录），relay/智能客户端按序消费
- 凭证不经 spec：writer 使用引擎进程的静态 MinIO 配置（s3a key，引擎启动时注入、限定结果前缀只写）；未来每查询凭证经 spec 扩展字段下发（proto 预留位）
- 行→Arrow 转换成本在 executor（DSv2 行接口固有），攒批大小由 `fg.result.batch.max.records` 控制
- driver 崩溃残留：retention 按"无 manifest 注册"清扫 part

**写入语义（两路线共用）**：

- **写并行化到 executor**：每个 task 的 `DataWriter` 行→Arrow 向量攒批、直写自己的 `part-{p}-{attempt}.arrows`，driver 不在数据路径（连 Arrow 编码都在 executor）
- **part 元数据走 Spark 原生 commit 通道**：`writer.commit()` 返回 `WriterCommitMessage{PartMetadata}`，driver `BatchWrite.commit(messages)` 聚合成 manifest——对标 Iceberg `DataFile → SnapshotSummary`
- **重试/清理白拿**：task retry 以 attemptId 命名 part 不互踩；query 失败 `abort(messages)` 按精确清单删除
- **为什么移除 S3 Compose**：Arrow IPC stream 不能字节级拼接（reader 在首个 EOS 停止），服务端 Compose 需要帧级手术，实现脆弱；由写入端分区数直接决定单/多对象
- `coalesce(1)` 而非 `repartition(1)`：单文件场景不需要 round-robin shuffle；代价是写入吞吐受单 task IO 限制（明码标价的取舍）
- 空分区不产文件、manifest 跳过（避免 0 行 part 污染 endpoint 列表）
- `limit` 注意（Kyuubi 教训）：不做 `df.limit()` 包装（避免最外层 limit 为 write 引入额外 shuffle）
- **顺序保证（H4）**：结果顺序 = manifest 的 partition index 序 + part 内行序。顶层 ORDER BY 的正确性依赖：① Spark RangePartitioner 的分区间有序（自然分区时）；② 禁用 `repartition(K)`（round-robin 摧毁序，§4.4.3 强制覆盖）；`coalesce(1)` 收敛单分区恒安全。无排序查询的 part 顺序即任意分区序

> **为什么 v0.2 需要增量迭代器、v0.3 不需要**：增量迭代器 = `df.toLocalIterator()` / Kyuubi `incrementalCollectResult` 语义——消费者每 `next()` 才向 executor 拉下一批，避免 `collect()` 把全量结果堆进 driver。v0.2 的 sink 在 driver 进程里逐批拉 Arrow 流写 MinIO（`ArrowConverters.toBatchIterator`），必须有它；v0.3 写入下沉到 executor task，driver 只收 KB 级 commit message，该问题消失。保留场景：未来"小结果 inline 直返"（不物化、走 relay 内存流）时仍用它。

#### 4.4.3 client（gateway 侧，基于 Spark Connect）

**plan 组装**——与官方 Scala 客户端 `spark.sql(sql).write.format("fg-result").mode("overwrite").save(path)` 生成的 plan 同构（即 `WriteOperation{input: SQL 节点}`，PoC ① 因此预验证通过）：

```scala
val input = partitionCfg match {                 // fg.result.partitions
  case None    => sqlRel(userSql)                // 自然分区：零额外数据移动
  case Some(1) => Coalesce(sqlRel, 1)            // 单对象（proto 若无 Coalesce 关系则 repartition(1)）
  case Some(k) => Repartition(k, sqlRel)         // 精确 K part：一次平衡 shuffle
}
ExecutePlan(root = WriteOperation(
  input   = CollectMetrics(input, "progress", count("*")),  // 实时行数进度
  path    = objectUri,
  source  = "fg-result",
  options = Map("spec" -> serialize(spec)),       // 物化参数经 DSv2 options 传递
  mode    = Overwrite))
```

- **触发时机**：首个 PollFlightInfo / GetFlightInfo 注册（OperationStore INSERT）后由触发实例异步提交——lazy，谁要结果谁触发
- 非查询语句（DDL/EXPLAIN/SET）：直传 SQL 文本通道，不做 plan 包装
- **ORDER BY 保护（H4，v0.13 收紧）**：AnalyzePlan 检测顶层全局排序——**mode=HTTPS 时强制 `coalesce(1)`**（spec：顺序敏感应返回单 endpoint）；relay 模式下单票流按 manifest 顺序天然有序，允许自然分区（依赖 RangePartitioner 分区间有序），禁 `repartition(K)`
- **queryId↔operationId 映射（H1）**：Connect 的取消与续流标识是 **operationId**（仅出现在 ExecutePlan 响应流首响应），非 gateway 的 queryId——触发实例首响应捕获后 **UPDATE 行 connect_operation_id**（任意实例可读，跨实例 CancelFlightInfo 因此可服务）；RECONCILING 期间保留（Reattach 同样用 operationId）
- **提交即 detach**：ExecutePlan（reattachable）首响应捕获 operationId → UPDATE 行 → **断开流**——触发实例自此对本查询无状态；引擎照常执行、manifest 照落（sink 是执行的一部分，与客户端连接无关）
- **取消（与 attach 解耦）**：任意实例 `Interrupt(operationId)`（不要求 attach，不影响在挂的 attach 者）+ CAS 行迁移；attach 者随后被动感知流终止，撞 CAS 空转，仅做 ReleaseExecution/本地清理；part 清理引擎侧 abort 自动；B 仅在无人 attach 且需观察终止时走正常租约 attach 收尾
- **元数据**：Connect Catalog 服务（GetDatabases/GetTables/GetTableSchema/Functions）→ `EngineCatalog` 适配（§4.8）
- **会话**：Connect 原生 session（每 session 独立 SessionState）；gateway session ↔ Connect session 一一映射

**进度分层**：

| 层 | 通道 | 时效/粒度 |
|---|---|---|
| 执行中·行数 | **ObservedMetrics**：`CollectMetrics("progress", count(*))` 在响应流中推送 | 实时，partition 完成粒度 |
| 终态·权威 | manifest（rowCount/bytes 精确）+ 响应流末尾 `MetricsObject`（write 节点 output rows）交叉验证 | 终态 |

不做执行中字节进度（不引入 gateway 对 MinIO 的对象轮询）；Connect 另有 QueryExecutionListener 终态事件转发，用于 trace 收尾。

**attach 模型（reattachable 主路径，PoC ⑤ 重点）**：

- poll 服务实例（任意一台）抢**行内 attach 租约**（`attach_owner` 条件 UPDATE + `attach_lease_until`，过期可接管）：
  - **赢家**：`Reattach(operationId)` 挂执行流——服务端记录的 offset 续传（含 ObservedMetrics）→ 流结束即完成 → UPDATE 行 + **`ReleaseExecution`**（释放引擎侧响应缓冲，义务）
  - **输家**：查行等待（`fg.poll.db.interval`）
- **并发约束**：Connect 同一 operation 仅允许一个活跃 attach——租约仲裁即为此而生；attach 者中途死亡，租约过期后被接管，offset 保证无损续接
- **无人 attach 的兜底**：引擎侧响应缓冲按保留 TTL 过期（要求 TTL ≥ poll 间隔，PoC ⑤ 验证项）；执行与物化不受影响
- **attach 不承载正确性**：引擎直写下响应流无数据（空 command result + 进度事件），持久事实是 manifest——完成信号兜底 = 等待超时查 manifest（`{resultKeyPrefix}/manifest.json` 存在 → 自助作答 + 修行为 COMPLETED）；**状态以 manifest 为准**，流报错但 manifest 在 → COMPLETED
- RECONCILING 仅作为 attach 者内部的短暂状态（Reattach 重试窗口），不落行

#### 4.4.4 回退路线 A：自有控制协议（PoC ③⑤ 失败时启用）

路线 B 的 PoC 硬性验证点（③ manifest 时序、⑤ 断流恢复）任一不成立时启用本路线。引擎 app 加厚（内嵌自有 OperationManager 与控制面），gateway client 走自有事件流协议：

```
app main（在薄壳的 1/3 步之外增加）:
  2'. SessionManager（会话=sparkSession + 隔离 conf）＋ OperationManager（有界执行池）
  3'. ControlPlane gRPC（替代 Connect server）:
        Submit{sql, MaterializationSpec} → stream EngineEvent{STATE/PROGRESS/COMPLETED/FAILED}
        Cancel{queryId} / Health / Catalog RPC(List*/...)（经 spark.catalog）

Submit 执行（driver 侧，即原 v0.7 §4.4.1 路径）:
  spark.sql(sql) → 列名去重 → partitionCfg match { None / Some(1)→coalesce(1) / Some(k)→repartition(k) }
  → write.format("fg-result").option("spec", ...).mode("overwrite").save()
  // commit 聚合 manifest → emit COMPLETED；失败/取消 → BatchWrite.abort + cancelJobGroup(queryId)（Kyuubi 同款）

client: 事件流映射 orchestrator future；周期 Health 探测（对标 Kyuubi GetInfo probe）
```

sink 件与路线 B **完全共用**；差异仅在引擎侧控制面厚度与 gateway client。

**A/B 取舍**：

| | A：自有控制协议 | B：Connect 薄壳（首选） |
|---|---|---|
| 自定义代码量 | 协议 + 厚 app + client | DSv2（共用）+ 薄壳 + plan 组装器 |
| 完成信号 | 自有事件流（断流即知） | 流结束 + Reattach + manifest 对账 |
| 进度 | 自定义事件（任意粒度） | ObservedMetrics（partition 完成粒度） |
| 版本耦合 | 自有协议自己管 | 绑定 Connect 协议演进（Spark 官方维护） |

**PoC 决策规则（v0.11 更新）**：

| PoC | 状态 | 失败处置 |
|---|---|---|
| ① WriteOperation 等价性 | **预验证通过**（官方 Scala 客户端 `spark.sql().write.format().save()` 即生成该 plan），退出风险清单 | — |
| ③ ExecutePlan 完成与 manifest 落 MinIO 的时序 | **硬性**，重点 | 回退 A |
| ⑤ reattachable 主路径：提交即 detach → 引擎继续执行/manifest 照落；Reattach offset 续传、并发 attach 行为、ReleaseExecution、无人 attach 的引擎侧保留 TTL | **硬性**，重点 | 回退 A |
| ② Connect 多 session 的 catalog/temp view 隔离度 | 软 | 退化：强制 USER share level |
| ④ ObservedMetrics 挂接 WriteOperation 的推送行为 | 软 | 退化：执行中无进度，仅终态 manifest |

SPI 不变，切换是 Kit 内政；§4.4.1-4.4.3（薄壳/sink/Connect client）中 sink 与生命周期机制在 A 路线全部复用。

### 4.5 引擎生命周期管理（Kyuubi 机制移植）

**engineSpace**——确定性命名，同 key 必得同路径：

```
/{fg.namespace}_{version}_{SHARE_LEVEL}_{engineType}/{routingUser}/{subdomain}
锁路径: /{...}_{SHARE_LEVEL}_{engineType}_lock/{routingUser}/{subdomain}
注册节点: serverUri=host:connectPort;version=...;refId=<uuid>;sequence=   （EPHEMERAL_SEQUENTIAL）
```

- share level：`USER`（默认）/ `GROUP` / `SERVER` / `CONNECTION`；subdomain 支持引擎池（`pool-{n}`，ZK DistributedAtomicInteger 轮转）
- **ensureEngine 三段式**（照抄 Kyuubi `EngineRef.getOrCreate`）：

```
1. 查: getChildren(engineSpace) → 有 → 取 connect host:port → Connect 探测（Config）→ 返回
2. 无 → 抢锁 InterProcessSemaphoreMutex(lockPath, timeout+10%)
     ├─ 锁内 double-check（别人刚建好 → 复用）
     ├─ SparkLauncher 启动引擎薄壳（deploy-mode=cluster；注入 HA_NAMESPACE、refId、白名单 conf、
     │    MinIO 静态 scoped 凭证[引擎级，非每查询]、fg-result jar）
     └─ 每 1s 轮询 getEngineByRefId(refId) 直到注册 或 initialize.timeout(默认120s)
3. 首连 refused → DEREGISTER 删 stale 节点 → 重试(默认3次)
```

- 全局拉起信号量 `fg.engine.launch.max.concurrent`；并发首查单引擎由锁 + double-check 保证
- 引擎侧自杀：idle timeout（默认 30m 无会话）、max lifetime、ZK 失联超宽限期

### 4.6 状态与寻址（DB 作业表 + manifest 分工）

**原则：执行状态进 DB，结果寻址出 manifest，gateway 零结果数据状态。**

- **OperationStore（§4.2 作业表）**：执行状态/operationId/mode 的持久层——跨实例 poll/cancel/触发实例死亡对账都靠它；量级=在途查询数，retention 清扫
- **manifest 即结果真相源**：引擎 commit 写入（schema/rowCount/bytes/part 清单），确定性路径 `{resultKeyPrefix}/manifest.json`；最终 poll / relay DoGet / RenewFlightEndpoint 都从它构造应答
- **relay ticket（H5 字段 + v0.16 扩展）**：HMAC-SHA256 信封 `{kind, bucket, resultKeyPrefix, queryId, user, issuedAt[, partIndex]}`——`kind=STREAM`（查询级，DoGet 按 manifest 顺序流全部 part；GetFlightInfo 快返路径唯一选项，完成前 part 数未知）与 `kind=PART`（分片级，携 partIndex，DoGet 只流该 part；最终 poll 按 manifest 铸 N 张票，单分片可重试）；manifest 路径直推；防伪造 + user 校验 + TTL 对齐
- **presign（https 模式）**：最终 poll / RenewFlightEndpoint 请求时 `stat` 校验 → per-part presigned GET URL（`expiration_time` 填 presign TTL）
- **跨实例等待**：长轮询 RPC 内有界间隔查行（`fg.poll.db.interval`，索引单行查询）——**无需 MinIO notification**（DB 已吸收其职责）；对账兜底 = 等待超时查 manifest
- **retention**：结果对象按 `fg.result.retention` 清扫 + 孤儿判定（前缀无 manifest 且超宽限）；DB 行同步清扫；也可下沉 MinIO 生命周期策略
- （可选优化）manifest 进程内短 LRU——缓存，可随时丢弃

### 4.7 Relay（兼容模式）

- `getStream(ticket)` → 验签 → 查行（在途则等待，同 poll 语义）→ 读 manifest：
  - `kind=STREAM`：依序 open 全部 part，单流顺序中继
  - `kind=PART`：只 open manifest 中该 partIndex 的 part
  → `ArrowStreamReader` 逐 batch → `putNextWhenClientReady()`（Dremio 背压策略：`isReady` 不满足则 wait，超时 `fg.relay.client.readiness.timeout` 默认 50s fail）
- 内存 O(1 batch)/流；流断开即关 HTTP 流；**容量**：relay 池按 并发查询 × 平均 part 数 规划（多 endpoint 并行 DoGet）

### 4.8 元数据目录命令（Flight SQL JDBC 兼容）

**命令族映射**（对标 Dremio `DremioFlightProducer` 的目录族实现）：

| Flight SQL 命令 | 数据来源（Spark） | 说明 |
|---|---|---|
| `CommandGetSqlInfo` | gateway 静态声明 | server name/version、read-only、无事务等能力位；JDBC 握手即读 |
| `CommandGetCatalogs` | 单一虚拟 catalog | Spark 无 catalog 概念，返回常量（如 `spark`） |
| `CommandGetDbSchemas` | `spark.catalog.listDatabases` | database ↔ schema |
| `CommandGetTables` | `spark.catalog.listTables`（含 temp view） | includeSchema 选项经 `listColumns` 补列 |
| `CommandGetTableTypes` | 常量表 | TABLE / VIEW / TEMP_VIEW / GLOBAL_TEMP_VIEW |
| `CommandGetXdbcTypeInfo` | 静态映射表 | Spark SQL 类型 ↔ XDBC 类型码 |
| PrimaryKeys / ExportedKeys / ImportedKeys / CrossReference | — | **返回空结果集**（非 UNIMPLEMENTED，BI/JDBC 对空结果更友好） |

**实现要点**：

- **数据通路**：v1 首选（路线 B）——**Connect 原生 Catalog 服务**（GetCatalogs/GetDatabases/GetTables/GetTableSchema/GetFunctions），client 件直接适配；回退（路线 A）——app 控制面 Catalog RPC（经 `spark.catalog`）。两侧均收敛到 `EngineCatalog` SPI
- **JDBC pattern 过滤**：`catalogPattern`/`schemaPattern`/`tableNamePattern`/`tableTypes` 在 gateway 侧翻译 `%`/`_` 通配（引擎侧不做过滤，保持 Kit 的 catalog API 简单）
- **结果返回路径**：目录结果行数百级，**gateway 内存构建 Arrow 直返 DoGet**（BasicResponseHandler 路径），不物化 MinIO、不占 relay 池；分页由 ticket 携带 offset 实现
- **temp view 可见性**：会话级对象按 sessionId 路由到对应引擎 session 查询（`listTables` 结果含 temp view，只对创建它的会话可见）
- **缓存**：目录族结果短 TTL 缓存（默认 60s），BI 工具翻目录高频调用全部覆盖：
  - `listDatabases` → key `{engine, user}`
  - `listTables`（persistent 部分）→ key `{engine, user, database}`；**temp view 部分不缓存**（会话级对象），每次实时查询后与缓存结果合并
  - `ListColumns`（表结构）→ key `{engine, user, table}`
  - **key 必须含 user**：GROUP/SERVER share level 下同一引擎服务多用户，可见性/脱敏可能因用户而异（Ranger、Unity Catalog 类授权体系），不含 user 会串缓存；USER level 下引擎本就按用户隔离，含 user 的 key 使缓存行为与 share level 解耦，语义统一

---

## 5. 服务框架（"扒 Dremio 骨架"清单）

| Dremio 组件 | 移植目标 | 说明 |
|---|---|---|
| `com.dremio.service.Service` | `FgService` | `start()/close()` 生命周期 |
| `SingletonRegistry` | 同名 | `registerSelf/registerProvider`，顺序 start、逆序 close |
| `javax.inject.Provider` 延迟注入 | 同名 | 解依赖环，不引 Guice |
| `DACDaemon/DACDaemonModule` 范式 | `GatewayDaemon` + `GatewayDaemonModule` | bootstrap → registry → start 全部 |
| `DremioConfig`（typesafe） | `GatewayConfig` | `reference.conf` ← `gateway.conf` ← `-Dfg.*` 覆盖 |
| `BootStrapContext` | 同名 | root allocator、config、全局 scheduler |
| BufferAllocator 树 | arrow-memory-netty | root → `flight/`、`relay/`、`control/` child，随 stream/session 关闭 |
| `ContextMigratingExecutorServiceMirror` | 同名 | RequestContext 跨线程池传播 |
| `CloseableThreadPool` + NamedThreadFactory | 同名 | 池统一命名 `fg-<purpose>-N`，registry 统一 shutdown |
| Telemetry + TracerFacade 思路 | micrometer + OTel | Prometheus exporter；queryId 全链路 span（gateway ↔ 引擎 app 关联） |

**引擎 app 内部同样采用此范式**（Service 化的 SessionManager/OperationManager/ControlPlane/ZkAgent，配置经启动 conf 注入），保证 gateway 与引擎工程风格一致。

**启动顺序**：

```
1 Config/BootStrapContext → 2 ZK → 3 Metrics/Trace → 4 ResultStore
→ 5 EngineLifecycleManager → 6 Orchestrator → 7 FlightSessionsManager → 8 FlightFrontend(对外)
```

**关键指标**：

```
fg.query.total{status} / fg.query.duration / fg.query.rows / fg.query.bytes
fg.engine.discovery.latency / fg.engine.launch.duration / fg.engine.pool.size{user}
fg.engine.broken.count / fg.result.presign.expired / fg.result.orphan.cleaned
fg.relay.active_streams / fg.relay.backpressure.wait_ms
fg.control.event.lag / fg.flight.session.active / fg.allocator.allocated_bytes{child}
```

---

## 6. 关键设计决策与风险

| # | 决策/风险 | 处置 |
|---|---|---|
| D1 | https endpoint 的客户端兼容性 | v25 已标准化（ticket 置空 + HTTP GET + Arrow IPC 假定），但**旧客户端实现可能不支持**该 scheme——默认 mode=relay，https 按 header/session option 开启；支持度由 PoC ⑥ 摸底 |
| D2 | 长查询等待语义 | **已消解（v0.13 标准化）**：PollFlightInfo 首 poll 快返（info 即取消凭证）、后续长等待（有上限分段）、完成一次性交付 endpoints；长等待不占 GetFlightInfo、不依赖自定义 action |
| D3 | 依赖冲突（arrow/grpc/protobuf/scala/spark） | BOM 统一；Kit 独立模块隔离；必要时 shade relocate protobuf |
| D4 | ZK 抖动误判引擎下线 | 引擎侧宽限期自杀；gateway 只信 Health 探测 + connection refused |
| D5 | **引擎持有 MinIO 凭证**（直写代价） | v1 启动时注入静态 scoped key（限定结果前缀、只写），**不经 spec 下发**；凭证轮换/撤销依赖引擎重启（idle 回收天然助轮换）；v2 每查询 presigned-PUT/STS 经 spec 扩展字段（proto 预留位，只加字段不破坏契约） |
| D6 | **引擎崩溃留半成品对象** | sink finally abort + retention 孤儿清扫双保险 |
| D7 | 完成信号可靠性（attach 模型） | **reattachable 主路径**：触发实例提交即 detach（无状态）；poll 服务实例经 DB attach 租约仲裁后 Reattach（offset 续传、ReleaseExecution 义务）；断流 ≠ 失败，**状态以 manifest 为准**（对账兜底）；gateway 执行期滚动重启零影响 |
| D8 | 控制协议选型（自有协议 vs Spark Connect 薄壳） | v1 路线 B 为主叙事（§4.4.1-4.4.3）；PoC ① WriteOperation 等价性**预验证通过**，硬性风险收敛为 ③ manifest 时序、⑤ 断流恢复——失败回退 A（§4.4.4）；SPI 不变 |
| D9 | 超大结果（**无字节熔断**，已按决策移除） | 唯一护栏 = 查询超时 `fg.query.timeout`；单 part 超 MinIO 5TB 对象上限自然失败；存储治理靠 retention 清扫 + 容量告警 |
| D10 | DoPut 参数绑定 | v1.1；v1 无参直查 + 预编译缓存 |
| D11 | **单对象 vs 多 part + 顺序保证（H4，v0.16 跨模式统一）** | `fg.result.partitions`：未设置=自然分区；=1=单对象；=K=平衡分区。**顺序敏感查询两种 mode 都返回单 endpoint**（spec"clients may ignore ordered"）：https 强制写端 `coalesce(1)`，relay 返回单 STREAM 票；无序查询 https=N URL、relay=N PART 票。已放弃 S3 Compose（IPC 流不可字节拼接） |
| D12 | 行→Arrow 攒批在 executor（DSv2 行接口） | 攒批大小受 `fg.result.batch.max.records` 控制；CPU 成本随 task 并行摊开，可接受 |
| D13 | relay ticket 自描述+签名（H5 + v0.16 双 kind） | HMAC 信封 `{kind=STREAM|PART, bucket, resultKeyPrefix, queryId, user, issuedAt[, partIndex]}`（manifest 路径直推）；STREAM=查询级（快返路径唯一选项），PART=分片级（最终 poll 铸 N 张）；防伪造、重启免疫；密钥轮换失效与 TTL 对齐 |
| D14 | **DB 作业表（新依赖）** | 执行状态/operationId/mode/attach 租约落库：跨实例 poll/cancel、重启存活、幂等首 poll（指纹唯一键）、attach 仲裁（条件 UPDATE）；行迁移由租约持有者驱动，滞后时 manifest 对账兜底；跨实例等待 = 长轮询内有界查行（索引单行）——**MinIO notification 退役** |
| D15 | **endpoint 模式不可混发**（endpoints=分片拼接语义，混发=数据翻倍） | mode（HTTPS/RELAY）注册时协商（header/session option）落行，最终 FlightInfo 按行构造 |
| D16 | 客户端标准支持矩阵 | pyarrow/ADBC/JDBC 对 PollFlightInfo、https Location、RenewFlightEndpoint 的支持度 = PoC ⑥，决定默认 mode 与 GetFlightInfo 兼容路径权重；**另验 endpoints 消费行为**（是否遍历全部分片 endpoint 拼接——只取首个的非合规客户端需 `fg.result.relay.single-stream` 兜底） |

---

## 7. 配置项清单（节选）

| key | 默认 | 说明 |
|---|---|---|
| `fg.flight.port` / `fg.flight.tls.enabled` | 32010 / true | 对外端口/TLS |
| `fg.zk.addresses` / `fg.zk.namespace` | — / `flight-gateway` | 发现与锁 |
| `fg.engine.share.level` / `fg.engine.share.subdomain` | USER / default | 引擎粒度 |
| `fg.engine.initialize.timeout` / `fg.engine.idle.timeout` | 120s / 30m | 引擎生命周期 |
| `fg.engine.launch.max.concurrent` | 5 | 全局拉起信号量 |
| `fg.engine.creds.mode` | launch-static | v1 仅启动注入静态 scoped key；预留 per-query-presigned（v2，经 spec 扩展字段） |
| `fg.result.bucket` / `fg.result.presign.ttl` / `fg.result.retention` | — / 1h / 24h | 结果对象 |
| `fg.query.timeout` | 600s | 查询护栏（唯一护栏；无字节熔断） |
| `fg.poll.max-wait` | 60s | 单次 poll 长等待上限（到点返回未完成，客户端续 poll） |
| `fg.poll.db.interval` | 2s | 跨实例长等待的查行间隔（索引单行查询） |
| `fg.result.endpoint.mode` | relay | HTTPS/RELAY；可被客户端 header/session option 覆盖（注册时落行，D15） |
| `fg.query.reconcile.window` | 60s | 断流对账窗口（Reattach 重试 + manifest 检查的总预算） |
| `fg.result.batch.max.records` | 16384 | 透传 `spark.sql.execution.arrow.maxRecordsPerBatch` |
| `fg.result.partitions` | 未设置 | 最终 part 数（=endpoint 数）：未设置=自然分区；1=单对象(coalesce)；K=精确 K part(repartition) |
| `fg.relay.enabled` / `fg.relay.client.readiness.timeout` | true / 50s | 中继 |
| `fg.result.relay.single-stream` | false | relay 最终 poll 返回单 STREAM 票（默认 N 张 PART 票）——非合规客户端兜底 |
| `fg.executor.relay.size` | 64 | relay 有界池（按 并发查询 × 平均 part 数 规划） |

---

## 8. 里程碑

| 阶段 | 内容 | 出口标准 |
|---|---|---|
| **M1 打通** | Dremio 骨架 + Flight 前端（SqlInfo/直查/**PollFlightInfo/CancelFlightInfo/RenewFlightEndpoint**）+ OperationStore(DB) + Spark Kit（薄壳+sink+Connect client，单固定引擎）+ relay + https presign；**第一周：路线 B PoC 聚焦硬性两点（③ manifest 时序、⑤ 断流恢复/Reattach）+ ⑥ 客户端标准支持矩阵（PollFlightInfo/https Location/RenewFlightEndpoint 的 pyarrow/ADBC/JDBC 支持度）**，据此定 A/B 与默认 mode | pyarrow query→fetch；首 poll 快返+触发、后续 poll 长等待、最终一次性 endpoints（双 mode）、CancelFlightInfo 在途取消全链路 |
| **M2 引擎管理+元数据** | ZK 发现+锁+拉起+stale+探测；USER/GROUP share level；元数据目录族（Catalog RPC + JDBC pattern + SqlInfo） | 并发首查单引擎；kill 引擎自动恢复；**JDBC DatabaseMetaData 浏览正常** |
| **M3 生产化** | 双模式、预编译+参数绑定、TLS/认证、HA、retention/孤儿清扫、压测（含 relay 背压，参考 Dremio `ITBackPressure`） | **ADBC + Flight SQL JDBC 全流程** + 故障注入通过 |

---

## 9. 测试策略

- **单测**：engineSpace 路径函数、状态机、MaterializationSpec→objectKey、背压 handler（仿 `TestBackpressureHandlingResponseHandler`）
- **sink 集成**：本地 MinIO + `spark.local[2]`：part IPC 格式正确性（pyarrow `ipc.open_stream` 校验）、manifest 顺序 = partition 顺序、abort 清单删除、task retry 不互踩、`partitions=1` 单流直读、`partitions=K` 文件数精确、空分区不产文件
- **端到端**：testcontainers（MinIO + ZK）；ADBC / pyarrow / **Flight SQL JDBC** 三客户端（JDBC 侧重 DatabaseMetaData 全族 + pattern 过滤）
- **故障注入**：引擎 kill（半成品清扫）、ZK 断连、MinIO 写中断、控制事件流断开、慢消费（relay 背压）、presign 过期
- **并发**：N 并发首查单引擎（锁正确性）；引擎池轮转均匀性
