package org.fg.spark.client

import com.google.protobuf.ByteString
import io.grpc.Status
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.connect.proto._
import org.fg.common.config.GatewayConfig
import org.fg.spi._

import java.time.Duration
import java.util.concurrent.CompletableFuture

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
 */
final class SparkEngineClient(config: GatewayConfig) extends SqlEngine {

  private val channel = new ConnectChannel(config.getString("fg.engine.spark.connect.uri"))

  override def `type`(): String = "spark"

  override def openSession(ctx: GatewaySession): EngineSession =
    new SparkEngineSession(ctx, channel)

  override def close(): Unit = channel.close()

  // ------------------------------------------------------------- AnalyzePlan

  override def analyzeSchema(session: EngineSession, sql: String, timeout: Duration): Schema = {
    val s = session.asInstanceOf[SparkEngineSession]
    val request =
      MaterializationPlanner.analyzePlanRequest(s.user, s.gatewaySessionId, sql)
    val response = channel
      .unary[AnalyzePlanRequest, AnalyzePlanResponse](
        SparkConnectServiceGrpc.getAnalyzePlanMethod,
        request,
        s.user,
        s.gatewaySessionId,
        timeout.toMillis)
      .get()
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
      listener: SqlEngine.SubmitListener): CompletableFuture[EngineExecutionHandle] = {
    val s = session.asInstanceOf[SparkEngineSession]
    val request = MaterializationPlanner.executePlanRequest(s.user, s.gatewaySessionId, sql, spec)
    val future = new CompletableFuture[EngineExecutionHandle]()

    lazy val cancel: () => Unit = channel.serverStream[ExecutePlanRequest, ExecutePlanResponse](
      SparkConnectServiceGrpc.getExecutePlanMethod,
      request,
      s.user,
      s.gatewaySessionId,
      onMessage = (resp: ExecutePlanResponse) => {
        val opId = resp.getOperationId
        if (opId != null && !opId.isEmpty && !future.isDone) {
          val handle = new EngineExecutionHandle("spark", opId)
          listener.onHandle(handle)
          future.complete(handle)
          // 提交即 detach：拿到 operationId 即断流（引擎照常执行、manifest 照落，⑤ 主路径）
          cancel()
        }
        // 忽略后续响应（ObservedMetrics 等由 attach 者消费）
      },
      onDone = (status: Status) => {
        if (!future.isDone) {
          if (status.isOk) {
            // 流正常结束但未捕获 operationId：执行或已终态，交由 manifest 对账判定
            future.completeExceptionally(
              new IllegalStateException(s"execute stream ended without operationId: $status"))
          } else {
            future.completeExceptionally(
              new IllegalStateException(s"execute stream failed: $status"))
          }
        }
      }
    )
    future
  }

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
    channel.serverStream[ReattachExecuteRequest, ExecutePlanResponse](
      SparkConnectServiceGrpc.getReattachExecuteMethod,
      request,
      s.user,
      s.gatewaySessionId,
      onMessage = (_: ExecutePlanResponse) => {
        // 消费响应流（含 ObservedMetrics/进度）；offset 续传由服务端维护
      },
      onDone = (status: Status) => {
        val outcome =
          if (status.isOk) ExecutionOutcome.completed()
          else if (Status.Code.CANCELLED == status.getCode) ExecutionOutcome.cancelled()
          else ExecutionOutcome.unknown(s"reattach stream closed: $status")
        future.complete(outcome)
      }
    )
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
      .unary[InterruptRequest, InterruptResponse](
        SparkConnectServiceGrpc.getInterruptMethod,
        request,
        s.user,
        s.gatewaySessionId,
        30_000L)
      .get()
  }

  override def releaseExecution(handle: EngineExecutionHandle): Unit = {
    // release 按 operationId 定位（attach 者义务）；已清理/不存在时幂等忽略
    val request = ReleaseExecuteRequest.newBuilder()
      .setUserContext(UserContext.newBuilder().setUserId("fg-gateway"))
      .setClientType("fg-gateway")
      .setOperationId(handle.handle())
      .setReleaseAll(
        org.apache.spark.connect.proto.ReleaseExecuteRequest.ReleaseAll.newBuilder().build())
      .build()
    try {
      channel
        .unary[ReleaseExecuteRequest, ReleaseExecuteResponse](
          SparkConnectServiceGrpc.getReleaseExecuteMethod,
          request,
          "fg-gateway",
          "",
          30_000L)
        .get()
    } catch {
      case _: Exception => // 幂等
    }
  }

  override def catalog(session: EngineSession): EngineCatalog = EmptyEngineCatalog

  /** M1 占位（M2 接 Connect Catalog 服务，design §4.8）。 */
  object EmptyEngineCatalog extends EngineCatalog {
    override def listDatabases(): java.util.List[EngineCatalog.EngineDatabase] =
      java.util.List.of()
    override def listTables(database: String): java.util.List[EngineCatalog.EngineTable] =
      java.util.List.of()
    override def listColumns(
        database: String,
        table: String): java.util.List[EngineCatalog.EngineColumn] = java.util.List.of()
  }
}

/** Connect 会话：gateway 会话 ↔ Connect session 一一映射（sessionId 同值传递）。 */
final class SparkEngineSession(val ctx: GatewaySession, channel: ConnectChannel)
    extends EngineSession {
  val user: String = ctx.user()
  val gatewaySessionId: String = ctx.sessionId()

  override def sessionId(): String = gatewaySessionId

  override def close(): Unit = {
    // Connect server 会话由空闲回收；gateway 侧无额外状态
  }
}
