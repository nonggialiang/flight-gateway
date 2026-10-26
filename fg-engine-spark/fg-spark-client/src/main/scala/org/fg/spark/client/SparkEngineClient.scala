package org.fg.spark.client

import io.grpc.stub.StreamObserver
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.connect.proto._
import org.fg.common.config.GatewayConfig
import org.fg.spi._

import scala.collection.JavaConverters._

import java.time.Duration
import java.util.concurrent.CompletableFuture

import org.slf4j.LoggerFactory

/**
 * Spark Engine Kit 的 gateway 侧 SqlEngine 实现（薄 stub，design §4.4.3）：
 *
 * <ul>
 *   <li>submit：ExecutePlan(reattachable)——首响应捕获 operationId 回调 listener 后**取消流**
 *       （提交即 detach；引擎照常执行、manifest 照落）；</li>
 *   <li>attach：ReattachExecute 挂执行流至终态（offset 续传；ReleaseExecution 义务在
 *       orchestrator 侧经 {@link #releaseExecution}）；流断≠失败（UNKNOWN 由 manifest 对账兜底）；</li>
 *   <li>interrupt：InterruptRequest（不要求 attach）。</li>
 * </ul>
 *
 * <p>单引擎客户端（M1 固定模式 / M2 每个被管引擎一个实例——SparkEngineManager 持有多个）。
 *
 * @param config     网关配置
 * @param connectUri 本引擎 Connect 地址（sc://host:port）
 * @param adminPort  本引擎 FgSessionAdmin HTTP 端口（主机同 connectUri）
 */
final class SparkEngineClient(config: GatewayConfig, connectUri: String, adminPort: Int) extends SqlEngine {

  private val logger = LoggerFactory.getLogger(classOf[SparkEngineClient])

  private val channel = new ConnectChannel(connectUri)

  /** M1 固定引擎模式（fg.zk.addresses 为空）：单固定 URI + 配置端口。 */
  def this(config: GatewayConfig) =
    this(config, config.getString("fg.engine.spark.connect.uri"), config.getInt("fg.engine.spark.admin.port"))

  override def `type`(): String = "spark"

  override def openSession(ctx: GatewaySession): EngineSession =
    new SparkEngineSession(ctx)

  override def close(): Unit = channel.close()

  // ------------------------------------------------------------- AnalyzePlan

  override def analyzeSchema(session: EngineSession, sql: String, timeout: Duration): Schema = {
    val s = session.asInstanceOf[SparkEngineSession]
    val stub = channel
      .unaryStub[SparkConnectServiceGrpc.SparkConnectServiceBlockingStub](
        ch => SparkConnectServiceGrpc.newBlockingStub(ch),
        s.user, s.gatewaySessionId, timeout.toMillis)
      .withDeadlineAfter(timeout.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
    val response = stub.analyzePlan(
      MaterializationPlanner.analyzePlanRequest(s.user, s.gatewaySessionId, sql))
    val dt = response.getSchema.getSchema
    if (dt != null && dt.getKindCase == DataType.KindCase.STRUCT) {
      MaterializationPlanner.toArrowSchema(dt.getStruct)
    } else {
      null
    }
  }

  override def isOrderSensitive(session: EngineSession, sql: String): Boolean =
    SqlOrderHeuristics.isOrderSensitive(sql)

  // ------------------------------------------------------------- 提交即 detach

  override def submit(
      session: EngineSession,
      sql: String,
      spec: MaterializationSpec,
      clientHandle: EngineExecutionHandle,
      listener: SqlEngine.SubmitListener): CompletableFuture[EngineExecutionHandle] = {
    val s = session.asInstanceOf[SparkEngineSession]
    // D36：预生成 operationId 随请求下发（引擎 UUID 规范化后作执行身份）；行上自 INSERT 起即带
    val request = MaterializationPlanner.executePlanRequest(
      s.user, s.gatewaySessionId, sql, spec, clientHandle.handle())
    val future = new CompletableFuture[EngineExecutionHandle]()
    // D39 后补：手工持有 ClientCall——ClientCall.cancel public 且必定生效（原 CancelCapture
    // 拦截器捕获是 no-op，断点实证 observer=null，"提交即 detach"从未生效）
    val callRef = new java.util.concurrent.atomic.AtomicReference[
        io.grpc.ClientCall[ExecutePlanRequest, ExecutePlanResponse]]()
    val observer = new StreamObserver[ExecutePlanResponse] {
      private var done = false

      override def onNext(value: ExecutePlanResponse): Unit = synchronized {
        if (done) return
        val opId = value.getOperationId
        if (opId != null && !opId.isEmpty && !future.isDone) {
          done = true
          // 回显校验：预生成 id 与引擎回显不一致 = 引擎侧异常形态，warn 供排查（行上仍以预生成值为准）
          if (!opId.equalsIgnoreCase(clientHandle.handle())) {
            logger.warn(
              s"engine echoed different operationId: echoed=$opId pregenerated=${clientHandle.handle()}")
          }
          try listener.onHandle(clientHandle)
          catch {
            case _: Throwable => // 触发实例回调异常不阻断 detach
          }
          future.complete(clientHandle)
          // 提交即 detach（⑤ 主路径）：引擎照常执行、manifest 照落、响应进缓冲待 attach 重放
          val call = callRef.get()
          if (call != null) {
            call.cancel("fg: detached after operationId capture", null)
          }
        }
        // 其余响应（ObservedMetrics 等）由 attach 者消费
      }

      override def onError(t: Throwable): Unit = synchronized {
        if (!future.isDone) {
          future.completeExceptionally(t)
        }
      }

      override def onCompleted(): Unit = synchronized {
        if (!future.isDone) {
          // 流正常结束但未捕获 operationId：执行或已终态，交由 manifest 对账判定
          future.completeExceptionally(
            new IllegalStateException("execute stream completed without operationId"))
        }
      }
    }
    callRef.set(channel.startServerStreaming(
      SparkConnectServiceGrpc.getExecutePlanMethod, s.user, s.gatewaySessionId, request, observer))
    future
  }

  // ------------------------------------------------------------- D38 inline（流式取数）

  override def submitInline(
      session: EngineSession,
      sql: String,
      clientHandle: EngineExecutionHandle,
      listener: SqlEngine.SubmitListener): CompletableFuture[EngineExecutionHandle] = {
    val s = session.asInstanceOf[SparkEngineSession]
    val request = MaterializationPlanner.inlinePlanRequest(
      s.user, s.gatewaySessionId, sql, clientHandle.handle())
    val future = new CompletableFuture[EngineExecutionHandle]()
    // D39 后补：同 submit——手工持有 ClientCall，cancel 必定生效（inline 的重放消费者
    // 依赖 detach 成立：未取消的 submit 流会吃掉全部活消息，reattach 只拿到缓冲重放）
    val callRef = new java.util.concurrent.atomic.AtomicReference[
        io.grpc.ClientCall[ExecutePlanRequest, ExecutePlanResponse]]()
    val observer = new StreamObserver[ExecutePlanResponse] {
      private var done = false
      override def onNext(value: ExecutePlanResponse): Unit = synchronized {
        if (done) return
        val opId = value.getOperationId
        if (opId != null && !opId.isEmpty && !future.isDone) {
          done = true
          try listener.onHandle(clientHandle)
          catch { case _: Throwable => }
          future.complete(clientHandle)
          val call = callRef.get() // 提交即 detach（同 submit）
          if (call != null) {
            call.cancel("fg: detached after operationId capture", null)
          }
        }
      }
      override def onError(t: Throwable): Unit = synchronized {
        if (!future.isDone) future.completeExceptionally(t)
      }
      override def onCompleted(): Unit = synchronized {
        if (!future.isDone) {
          future.completeExceptionally(
            new IllegalStateException("inline execute stream completed without operationId"))
        }
      }
    }
    callRef.set(channel.startServerStreaming(
      SparkConnectServiceGrpc.getExecutePlanMethod, s.user, s.gatewaySessionId, request, observer))
    future
  }

  override def attachInline(
      session: EngineSession,
      handle: EngineExecutionHandle,
      lastResponseId: String,
      consumer: SqlEngine.InlineConsumer): CompletableFuture[ExecutionOutcome] = {
    val s = session.asInstanceOf[SparkEngineSession]
    val future = new CompletableFuture[ExecutionOutcome]()
    val b = ReattachExecuteRequest.newBuilder()
      .setSessionId(s.gatewaySessionId)
      .setUserContext(UserContext.newBuilder().setUserId(s.user))
      .setClientType("fg-gateway")
      .setOperationId(handle.handle())
    if (lastResponseId != null) {
      b.setLastResponseId(lastResponseId) // D38:断线续传游标(空=从头消费)
    }
    val async = channel.serverStreamingStub[SparkConnectServiceGrpc.SparkConnectServiceStub](
      ch => SparkConnectServiceGrpc.newStub(ch),
      s.user, s.gatewaySessionId)
    async.reattachExecute(b.build(), new StreamObserver[ExecutePlanResponse] {
      override def onNext(value: ExecutePlanResponse): Unit = {
        // 观察者绝不抛（executeCommand 同款教训：抛=grpc 吞异常伪装 CANCELLED）
        try {
          if (value.hasSchema && value.getSchema.hasStruct) {
            consumer.onSchema(
              MaterializationPlanner.toArrowSchema(value.getSchema.getStruct))
          } else if (value.hasArrowBatch) {
            val batch = value.getArrowBatch
            consumer.onBatch(value.getResponseId, batch.getData.toByteArray)
          } else if (value.hasResultComplete) {
            consumer.onResultComplete()
          }
          // 其余（ObservedMetrics 等）忽略
        } catch {
          case t: Throwable =>
            if (!future.isDone) {
              future.complete(ExecutionOutcome.unknown("inline consumer failed: " + t))
            }
        }
      }
      override def onError(t: Throwable): Unit = {
        future.complete(SparkEngineClient.classifyInlineAttachError(t))
      }
      override def onCompleted(): Unit = {
        future.complete(ExecutionOutcome.completed())
      }
    })
    future
  }

  override def releaseUntil(handle: EngineExecutionHandle, responseId: String): Unit = {
    val request = ReleaseExecuteRequest.newBuilder()
      .setUserContext(UserContext.newBuilder().setUserId("fg-gateway"))
      .setClientType("fg-gateway")
      .setOperationId(handle.handle())
      .setReleaseUntil(ReleaseExecuteRequest.ReleaseUntil.newBuilder().setResponseId(responseId))
      .build()
    try {
      channel
        .unaryStub[SparkConnectServiceGrpc.SparkConnectServiceBlockingStub](
          ch => SparkConnectServiceGrpc.newBlockingStub(ch),
          "fg-gateway", "", 30000L)
        .withDeadlineAfter(30, java.util.concurrent.TimeUnit.SECONDS)
        .releaseExecute(request)
    } catch {
      case _: Exception => // 幂等：已释放/不存在忽略
    }
  }

  // ------------------------------------------------------------- 命令执行（D17/D18）

  /**
   * 命令执行（非 SELECT）：无物化、结果小、持流至终态（流终即权威事实，无 UNKNOWN）。
   *
   * <p>请求为 Plan.root=SQL 关系（无 WriteOperation/无 ReattachOptions，见
   * {@link MaterializationPlanner#commandPlanRequest}）——Spark 把 SET/SHOW/DESCRIBE/
   * EXPLAIN/USE/DDL/DML 当普通 DataFrame 执行：schema 消息 + arrow_batch 消息 + 流结束。
   * 响应重组为完整 Arrow IPC stream bytes（schema message 先写、逐批 writeBatch）内联
   * 返还编排层。守卫：{@code fg.command.timeout}（自管超时，无 gRPC deadline）与
   * {@code fg.command.result.max-bytes}（累计 batch 字节熔断），二者到点 cancel 流 + FAILED。
   *
   * <p>注意：result_complete 只在 reattachable 执行下发（ExecuteThreadRunner 按此门控），
   * 本路径非 reattachable——完成信号以 onCompleted 为准，result_complete 仅置位记录。
   */
  override def executeCommand(
      session: EngineSession,
      sql: String,
      listener: SqlEngine.SubmitListener): CompletableFuture[CommandOutcome] = {
    val s = session.asInstanceOf[SparkEngineSession]
    val request = MaterializationPlanner.commandPlanRequest(s.user, s.gatewaySessionId, sql)
    val future = new CompletableFuture[CommandOutcome]()
    // D39 后补：同 submit——手工持有 ClientCall（fail/超时熔断的 cancel 必定生效；
    // 原 CancelCapture no-op 导致命令超时/结果超限时引擎流从未被取消，命令在引擎侧裸跑）
    val callRef = new java.util.concurrent.atomic.AtomicReference[
        io.grpc.ClientCall[ExecutePlanRequest, ExecutePlanResponse]]()

    val timeoutMs = config.getDurationMs("fg.command.timeout")
    val maxBytes = config.getLong("fg.command.result.max-bytes")

    // 单命令生命周期内的累积器：独立 RootAllocator（终态必 close 释放）+ 输出 IPC 流
    val allocator = new org.apache.arrow.memory.RootAllocator(Long.MaxValue)
    val ipcOut = new java.io.ByteArrayOutputStream()
    var root: org.apache.arrow.vector.VectorSchemaRoot = null
    var writer: org.apache.arrow.vector.ipc.ArrowStreamWriter = null
    var schema: Schema = null // 首个 STRUCT schema；命令无结果集时合成 [ok BOOLEAN]
    var handleSeen = false
    var resultComplete = false
    var totalBatchBytes = 0L

    def closeResources(): Unit = synchronized {
      try { if (writer != null) writer.close() }
      catch { case _: Throwable => }
      try { if (root != null) root.close() }
      catch { case _: Throwable => }
      try { allocator.close() }
      catch { case _: Throwable => }
    }

    def fail(message: String): Unit = {
      val call = callRef.get()
      if (call != null) {
        call.cancel("fg: command failed: " + message, null)
      }
      closeResources()
      future.complete(CommandOutcome.failed(message))
    }

    // 自管超时（无 gRPC deadline：deadline 会把长命令一刀切且无法区分超时/引擎错）
    val timeoutTask = SparkEngineClient.commandTimeoutScheduler.schedule(
      new Runnable {
        override def run(): Unit = fail(s"command timeout after ${timeoutMs}ms")
      },
      timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)

    val observer = new StreamObserver[ExecutePlanResponse] {
      override def onNext(value: ExecutePlanResponse): Unit = synchronized {
        // 观察者绝不可抛：grpc-java 会因此 cancel 流并以 CANCELLED 回调 onError，
        // 真实异常被吞（曾把 schema/batch 重组失败伪装成"Query was cancelled"）。
        try {
          val opId = value.getOperationId
          if (!handleSeen && opId != null && !opId.isEmpty) {
            handleSeen = true
            try listener.onHandle(new EngineExecutionHandle("spark", opId))
            catch { case _: Throwable => } // 回调异常不阻断流消费
          }
          val dt = value.getSchema
          // 零字段 STRUCT（DDL 类：spark.sql 的 DataFrame 无列）不采作交付 schema——
          // 回退 [ok BOOLEAN] 合成（Dremio 式非空 schema 承诺，D18）
          if (schema == null && dt != null && dt.getKindCase == DataType.KindCase.STRUCT
              && !dt.getStruct.getFieldsList.isEmpty) {
            schema = MaterializationPlanner.toArrowSchema(dt.getStruct)
          }
          if (value.hasArrowBatch) {
            val data = value.getArrowBatch.getData
            totalBatchBytes += data.size()
            if (totalBatchBytes > maxBytes) {
              fail(s"command result exceeds fg.command.result.max-bytes ($maxBytes): $totalBatchBytes")
              return
            }
            if (root == null) {
              root = org.apache.arrow.vector.VectorSchemaRoot.create(
                if (schema != null) schema else SparkEngineClient.okSchema, allocator)
              writer = new org.apache.arrow.vector.ipc.ArrowStreamWriter(root, null, ipcOut)
              writer.start() // IPC 流先写 schema message
            }
            // arrow_batch.data 是完整单批 IPC stream（schema message + record batch + EOS，
            // ArrowConverters.toBatchWithSchemaIterator 的产出形态）——按流读取后装载到输出
            val reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(
              new java.io.ByteArrayInputStream(data.toByteArray), allocator)
            try {
              val src = reader.getVectorSchemaRoot
              while (reader.loadNextBatch()) {
                if (!src.getSchema.getFields.isEmpty) { // 0 字段批（DDL 类）恒 0 行，跳过
                  val batch = new org.apache.arrow.vector.VectorUnloader(src).getRecordBatch()
                  try new org.apache.arrow.vector.VectorLoader(root).load(batch)
                  finally batch.close()
                  writer.writeBatch()
                }
              }
            } finally {
              reader.close()
            }
          }
          if (value.hasResultComplete) {
            resultComplete = true // 非 reattachable 不下发；仅置位记录（见方法 javadoc）
          }
        } catch {
          case e: Throwable =>
            fail(s"command response processing failed: $e")
        }
      }

      override def onError(t: Throwable): Unit = {
        timeoutTask.cancel(false)
        closeResources()
        val code = t match {
          case e: io.grpc.StatusRuntimeException => e.getStatus.getCode
          case _ => io.grpc.Status.Code.UNKNOWN
        }
        if (code == io.grpc.Status.Code.CANCELLED) {
          future.complete(CommandOutcome.cancelled()) // 自家 timeout 熔断已先完成 future，此处幂等落空
        } else {
          future.complete(CommandOutcome.failed(s"command stream error: $t"))
        }
      }

      override def onCompleted(): Unit = {
        timeoutTask.cancel(false)
        try {
          val finalSchema = if (schema != null) schema else SparkEngineClient.okSchema
          if (root == null) {
            // 零批次命令：仍交付非空 schema 的空 IPC 流（Dremio 式客户端兼容）
            root = org.apache.arrow.vector.VectorSchemaRoot.create(finalSchema, allocator)
            writer = new org.apache.arrow.vector.ipc.ArrowStreamWriter(root, null, ipcOut)
            writer.start()
          }
          writer.close() // EOS
          val schemaBytes = SparkEngineClient.serializeSchema(finalSchema)
          future.complete(CommandOutcome.completed(schemaBytes, ipcOut.toByteArray))
        } catch {
          case e: Throwable => future.complete(CommandOutcome.failed(s"command result assembly failed: $e"))
        } finally {
          closeResources()
        }
      }
    }
    callRef.set(channel.startServerStreaming(
      SparkConnectServiceGrpc.getExecutePlanMethod, s.user, s.gatewaySessionId, request, observer))
    future
  }

  // ------------------------------------------------------------- 会话生命周期（D20）

  /** 会话管理面 HTTP 客户端（fg-spark-app FgSessionAdmin；主机同 connectUri）。 */
  private lazy val adminHttp = java.net.http.HttpClient.newHttpClient()
  private lazy val adminBase = {
    val (host, _) = ConnectChannel.parse(connectUri)
    s"http://$host:$adminPort"
  }
  private lazy val confTimeoutMs = config.getDurationMs("fg.query.prepare.timeout")

  /**
   * 化身校验（无副作用——admin 事件登记查询，不经 Connect RPC：Spark Connect 收到未知
   * session_id 的请求会静默重建同 id 会话，普通 RPC 探测即污染）。
   */
  override def sessionStatus(session: EngineSession): EngineSessionStatus = {
    val s = session.asInstanceOf[SparkEngineSession]
    val uri = java.net.URI.create(
      s"$adminBase/session/status?user=${url(s.user)}&id=${url(s.gatewaySessionId)}")
    val resp = adminHttp.send(
      java.net.http.HttpRequest.newBuilder(uri).GET().timeout(
        java.time.Duration.ofMillis(confTimeoutMs)).build(),
      java.net.http.HttpResponse.BodyHandlers.ofString())
    if (resp.statusCode() != 200) {
      throw new IllegalStateException(s"session admin status ${resp.statusCode()}: ${resp.body()}")
    }
    val alive = "\"alive\":true".r.findFirstIn(resp.body()).isDefined
    val started = "\"startedAt\":(\\d+)".r.findFirstMatchIn(resp.body()).map(_.group(1).toLong).getOrElse(0L)
    new EngineSessionStatus(alive, started)
  }

  /** 引擎侧关会话（cache invalidate → removal listener → expireSession 正规清理）。 */
  override def closeSession(session: EngineSession): Unit = {
    val s = session.asInstanceOf[SparkEngineSession]
    val uri = java.net.URI.create(
      s"$adminBase/session/close?user=${url(s.user)}&id=${url(s.gatewaySessionId)}")
    val resp = adminHttp.send(
      java.net.http.HttpRequest.newBuilder(uri).POST(
        java.net.http.HttpRequest.BodyPublishers.noBody()).timeout(
        java.time.Duration.ofMillis(confTimeoutMs)).build(),
      java.net.http.HttpResponse.BodyHandlers.ofString())
    // 404 = 会话本不在引擎侧（已逐出/重启），关闭语义已达成
    if (resp.statusCode() != 200 && resp.statusCode() != 404) {
      throw new IllegalStateException(s"session admin close ${resp.statusCode()}: ${resp.body()}")
    }
  }

  /** 会话选项：即时代理到引擎会话 conf（Config RPC；set 与 unset 分请求，operation 单选）。 */
  override def setSessionConf(
      session: EngineSession, toSet: java.util.Map[String, String],
      toUnset: java.util.Set[String]): Unit = {
    val s = session.asInstanceOf[SparkEngineSession]
    if (toSet != null && !toSet.isEmpty) {
      val setOp = ConfigRequest.Operation.newBuilder()
        .setSet(ConfigRequest.Set.newBuilder()
          .addAllPairs(toSet.asScala.map { case (k, v) =>
            KeyValue.newBuilder().setKey(k).setValue(v).build()
          }.toSeq.asJava)
          .build())
        .build()
      configRpc(s, setOp)
    }
    if (toUnset != null && !toUnset.isEmpty) {
      val unsetOp = ConfigRequest.Operation.newBuilder()
        .setUnset(ConfigRequest.Unset.newBuilder()
          .addAllKeys(toUnset.asScala.toSeq.asJava)
          .build())
        .build()
      configRpc(s, unsetOp)
    }
  }

  /** 会话选项回读：Config GET（值实时取引擎会话；键不存在即缺席）。 */
  override def getSessionConf(
      session: EngineSession, keys: java.util.Collection[String]): java.util.Map[String, String] = {
    val s = session.asInstanceOf[SparkEngineSession]
    if (keys == null || keys.isEmpty) return java.util.Map.of()
    val getOp = ConfigRequest.Operation.newBuilder()
      .setGet(ConfigRequest.Get.newBuilder()
        .addAllKeys(keys.asScala.toSeq.asJava)
        .build())
      .build()
    val resp = configRpc(s, getOp)
    val out = new java.util.LinkedHashMap[String, String]()
    resp.getPairsList.asScala.foreach(kv => out.put(kv.getKey, kv.getValue))
    out
  }

  private def configRpc(s: SparkEngineSession, op: ConfigRequest.Operation): ConfigResponse = {
    val stub = channel
      .unaryStub[SparkConnectServiceGrpc.SparkConnectServiceBlockingStub](
        ch => SparkConnectServiceGrpc.newBlockingStub(ch),
        s.user, s.gatewaySessionId, confTimeoutMs)
      .withDeadlineAfter(confTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
    val request = ConfigRequest.newBuilder()
      .setSessionId(s.gatewaySessionId)
      .setUserContext(UserContext.newBuilder().setUserId(s.user))
      .setClientType("fg-gateway")
      .setOperation(op)
      .build()
    stub.config(request)
  }

  private def url(v: String): String = java.net.URLEncoder.encode(v, "UTF-8")

  // ------------------------------------------------------------- attach

  override def attach(
      session: EngineSession,
      handle: EngineExecutionHandle): CompletableFuture[ExecutionOutcome] = {
    val s = session.asInstanceOf[SparkEngineSession]
    val future = new CompletableFuture[ExecutionOutcome]()
    val request = ReattachExecuteRequest.newBuilder()
      .setSessionId(s.gatewaySessionId)
      .setUserContext(UserContext.newBuilder().setUserId(s.user))
      .setClientType("fg-gateway")
      .setOperationId(handle.handle())
      .build()
    val async = channel.serverStreamingStub[SparkConnectServiceGrpc.SparkConnectServiceStub](
      ch => SparkConnectServiceGrpc.newStub(ch),
      s.user, s.gatewaySessionId)

    async.reattachExecute(request, new StreamObserver[ExecutePlanResponse] {
      override def onNext(value: ExecutePlanResponse): Unit = {
        // 消费响应流（含 ObservedMetrics）；offset 续传由服务端维护
      }

      override def onError(t: Throwable): Unit = {
        future.complete(SparkEngineClient.classifyAttachError(t))
      }

      override def onCompleted(): Unit = {
        future.complete(ExecutionOutcome.completed())
      }
    })
    future
  }

  // ------------------------------------------------------------- interrupt / release

  override def interrupt(session: EngineSession, handle: EngineExecutionHandle): Unit = {
    val s = session.asInstanceOf[SparkEngineSession]
    val request = InterruptRequest.newBuilder()
      .setSessionId(s.gatewaySessionId)
      .setUserContext(UserContext.newBuilder().setUserId(s.user))
      .setClientType("fg-gateway")
      .setOperationId(handle.handle())
      .build()
    channel
      .unaryStub[SparkConnectServiceGrpc.SparkConnectServiceBlockingStub](
        ch => SparkConnectServiceGrpc.newBlockingStub(ch),
        s.user, s.gatewaySessionId, 30000L)
      .withDeadlineAfter(30, java.util.concurrent.TimeUnit.SECONDS)
      .interrupt(request)
  }

  override def releaseExecution(handle: EngineExecutionHandle): Unit = {
    // release 按 operationId 定位（attach 者义务）；已清理/不存在时幂等忽略
    val request = ReleaseExecuteRequest.newBuilder()
      .setUserContext(UserContext.newBuilder().setUserId("fg-gateway"))
      .setClientType("fg-gateway")
      .setOperationId(handle.handle())
      .setReleaseAll(ReleaseExecuteRequest.ReleaseAll.newBuilder().build())
      .build()
    try {
      channel
        .unaryStub[SparkConnectServiceGrpc.SparkConnectServiceBlockingStub](
          ch => SparkConnectServiceGrpc.newBlockingStub(ch),
          "fg-gateway", "", 30000L)
        .withDeadlineAfter(30, java.util.concurrent.TimeUnit.SECONDS)
        .releaseExecute(request)
    } catch {
      case _: Exception => // 幂等
    }
  }

  /** 目录（D21，M2）：SHOW CATALOGS/SCHEMAS/TABLES/DESCRIBE 经命令管道（动态 catalog）。 */
  override def catalog(session: EngineSession): EngineCatalog =
    new SparkEngineCatalog(config, this, session)
}

private[client] object SparkEngineClient {

  /**
   * attach 流 onError 的终态分流（D35）：
   *
   * <ul>
   *   <li>CANCELLED —— 被 interrupt（CancelFlightInfo/interrupt 路径）→ cancelled；</li>
   *   <li>UNAVAILABLE / DEADLINE_EXCEEDED —— 传输类（连接断/引擎不可达/超时），执行状态
   *       不可判定 → unknown，交 manifest 对账兜底（D7 真相源，行为与 D35 前一致）；</li>
   *   <li>NOT_FOUND / INVALID_ARGUMENT —— reattach 寻址不到该 operationId（D36 预生成 id 后
   *       可达）：可能是 submit 尚未到达引擎（attach 抢跑）或引擎重启清空——不可判定 →
   *       unknown（前缀 "operation not found"，orchestrator 侧按 orphan-grace 宽限后判死）；</li>
   *   <li>其余（INTERNAL / UNKNOWN / RESOURCE_EXHAUSTED…）—— 执行类：Spark 经
   *       GrpcExceptionConverter 把运行时异常作为流尾错误状态投递，且对已失败 operation 的
   *       reattach 重放恒复现同一错误 → 判定失败，<b>真实异常直达客户端</b>。修复此前
   *       "执行失败被归 UNKNOWN → 无 manifest → 反复重挂 → 600s sweeper 兜底且报 query
   *       timeout"的缺口（ExecutionOutcome.failed 此前是 SPI 死代码）。</li>
   * </ul>
   *
   * <p>安全性（双挂竞态）：releaseExecution 切断他人 attach 流的时序发生在持有者 casComplete
   * <b>之后</b>（whenComplete 的 switch 先于 finally release）——被切流者按本分流若判
   * failed，其 casFail 撞终态行条件更新 no-op，无误迁。
   */
  /**
   * D38:inline 消费 attach 的错误分流(与信号 attach 的 classifyAttachError 不同——
   * inline 错误直达客户端,INVALID_CURSOR 族是确定性失败而非 unknown):
   *   - INVALID_CURSOR.POSITION_NOT_AVAILABLE —— 游标已释放(前向单游标协议核心错误)
   *   - INVALID_CURSOR.POSITION_NOT_FOUND / NOT_REATTACHABLE / INVALID_CURSOR.DISCONNECTED
   *     —— 游标非法/操作不可重挂(被抢占),均为确定性失败
   *   - NOT_FOUND —— operation 不存在(submit 未达/已终态释放)→ unknown 交 FG 翻译 NOT_FOUND
   *   - UNAVAILABLE/DEADLINE_EXCEEDED —— 传输,可续传 → unknown(UNKNOWN 分支带 lastAcked 提示)
   *   - 其余 —— 引擎执行错 → failed(真实异常透传)
   */
  def classifyInlineAttachError(t: Throwable): ExecutionOutcome = {
    val message = if (t == null) "" else t.toString
    t match {
      case e: io.grpc.StatusRuntimeException =>
        val desc = Option(e.getStatus.getDescription).getOrElse("")
        val full = s"${e.getStatus.getCode}: $desc"
        if (full.contains("INVALID_CURSOR") || full.contains("NOT_REATTACHABLE")) {
          ExecutionOutcome.failed(s"invalid cursor: $full")
        } else if (full.contains("operation not found") || e.getStatus.getCode == io.grpc.Status.Code.NOT_FOUND) {
          ExecutionOutcome.unknown(s"operation not found: $full")
        } else {
          SparkEngineClient.classifyAttachError(t) // 其余沿用 D35 分流(传输→unknown/执行→failed)
        }
      case _ => ExecutionOutcome.unknown(s"reattach stream closed: $t")
    }
  }

  def classifyAttachError(t: Throwable): ExecutionOutcome = {
    t match {
      case e: io.grpc.StatusRuntimeException =>
        val desc = Option(e.getStatus.getDescription).getOrElse("")
        // Spark 包装:OPERATION_NOT_FOUND 以 INTERNAL gRPC status + SparkSQLException 文本
        // 送达——必须检查错误文本而非仅 status code(e2e 实证踩坑)
        if (desc.contains("OPERATION_NOT_FOUND") || desc.contains("operation not found")) {
          ExecutionOutcome.unknown(s"operation not found: ${e.getStatus.getCode}: $desc")
        } else {
          e.getStatus.getCode match {
            case io.grpc.Status.Code.CANCELLED =>
              ExecutionOutcome.cancelled()
            case io.grpc.Status.Code.UNAVAILABLE | io.grpc.Status.Code.DEADLINE_EXCEEDED =>
              ExecutionOutcome.unknown(
                s"reattach transport error: ${e.getStatus.getCode}: $desc")
            case io.grpc.Status.Code.NOT_FOUND | io.grpc.Status.Code.INVALID_ARGUMENT =>
              ExecutionOutcome.unknown(
                s"operation not found: ${e.getStatus.getCode}: $desc")
            case code =>
              ExecutionOutcome.failed(
                s"engine execution failed: $code: $desc")
          }
        }
      case _ =>
        ExecutionOutcome.unknown(s"reattach stream closed: $t")
    }
  }

  /** 命令超时守护线程（daemon 单线程：命令并发低，到点任务只做 cancel+complete）。 */
  private lazy val commandTimeoutScheduler =
    java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r => {
      val t = new Thread(r, "fg-cmd-timeout")
      t.setDaemon(true)
      t
    })

  /** 无结果集命令的合成 schema：[ok BOOLEAN]（D17/D18，与网关 CommandSchemas 的 DDL/USE 同形）。 */
  val okSchema: Schema = new Schema(
    java.util.List.of[org.apache.arrow.vector.types.pojo.Field](new org.apache.arrow.vector.types.pojo.Field(
      "ok",
      org.apache.arrow.vector.types.pojo.FieldType.nullable(
        new org.apache.arrow.vector.types.pojo.ArrowType.Bool()),
      null)))

  /** schema message 序列化——与网关 SchemaSerde.serialize 同 API 同字节。 */
  def serializeSchema(schema: Schema): Array[Byte] = {
    val out = new java.io.ByteArrayOutputStream()
    val channel = new org.apache.arrow.vector.ipc.WriteChannel(java.nio.channels.Channels.newChannel(out))
    org.apache.arrow.vector.ipc.message.MessageSerializer.serialize(channel, schema)
    out.toByteArray
  }
}

/** Connect 会话：gateway 会话 ↔ Connect session 一一映射（D20 生命周期绑定）。会话 id
 * 即客户端自报的 {@link GatewaySession#sessionId}（UUID 由网关入口强制，满足 Connect
 * INVALID_HANDLE.FORMAT 校验），零映射透传——化身事实由 fg_session 登记表裁决。
 * 引擎路由由 SparkEngineRouter 按调用重算，会话对象本身不持有 channel。 */
final class SparkEngineSession(val ctx: GatewaySession)
    extends EngineSession {
  val user: String = ctx.user()
  val gatewaySessionId: String = ctx.sessionId()

  override def sessionId(): String = gatewaySessionId

  override def close(): Unit = {
    // Connect 会话生命周期经 session-admin 显式管理（D20）；此处无 gateway 侧状态
  }
}
