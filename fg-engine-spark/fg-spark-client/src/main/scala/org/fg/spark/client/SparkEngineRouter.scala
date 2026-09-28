package org.fg.spark.client

import com.google.common.cache.CacheBuilder
import org.fg.common.config.GatewayConfig
import org.fg.ha.EngineSpaces
import org.fg.ha.FgZkClient
import org.fg.spi._

import java.net.{ConnectException, UnknownHostException}
import java.time.Duration
import java.util.Locale
import java.util.concurrent.{CompletableFuture, Executor}
import scala.collection.JavaConverters._

/**
 * ZK 模式 SqlEngine 装饰路由（D22 决策 2，SPI 零改动）：
 *
 * <ul>
 *   <li>每调用按 (shareLevel, routingUser(user), subdomain[, sessionId@CONNECTION]) 纯函数
 *       重算 engineSpace → manager.ensureEngine → 委派目标 SparkEngineClient。SERVER 路由
 *       常量用户、GROUP 经 GroupProvider、USER/CONNECTION 路由认证用户；CONNECTION 追加
 *       refId=fg 会话 id（space 内嵌 UUID，永不复用）；</li>
 *   <li><b>恢复只挂 sessionStatus</b>（决策 5，无副作用探针）：connect-refused →
 *       manager.recover（守卫注销 + 驱逐 + 重拉）→ 重试，界 fg.engine.open.max-attempts /
 *       retry-wait。submit/executeCommand/attach/interrupt 绝不自动重试——失败即失败，
 *       下一 RPC 的 resolveSession 自愈（防重复执行）；</li>
 *   <li><b>releaseExecution 无会话上下文</b>：router 维护 opId→engineSpace 有界映射
 *       （submit/executeCommand 的 SubmitListener 包装记录，release 时消费）；miss 则向
 *       全部在活 client 广播（幂等吞异常）；</li>
 *   <li><b>CONNECTION 引擎下线</b>（决策 4）：closeSession 委派 /session/close 后对
 *       CONNECTION 引擎 fire-and-forget POST /engine/stop（admin 平面是 local/cluster
 *       两模式统一通路）；兜底 = 引擎侧 never-connected fast-fail + idle 看门狗；</li>
 *   <li>closeSession/attach/interrupt 用既有引擎（existingClient）——死引擎上不做无谓
 *       冷启动；attach 无引擎 → UNKNOWN（manifest 对账兜底），interrupt 无引擎 → 抛异常。</li>
 * </ul>
 */
final class SparkEngineRouter(
    config: GatewayConfig,
    zk: FgZkClient,
    executor: Executor,
    meterRegistry: io.micrometer.core.instrument.MeterRegistry)
    extends SqlEngine {

  private val shareLevel = config.getString("fg.engine.share.level").trim.toUpperCase(Locale.ROOT)
  private val subdomain = config.getString("fg.engine.share.subdomain")
  private val namespace = config.getString("fg.zk.namespace")
  private val groupProvider: GroupProvider = new StaticGroupProvider(config)
  private val manager = new SparkEngineManager(config, zk, meterRegistry)
  private val openMaxAttempts = math.max(1, config.getInt("fg.engine.open.max-attempts"))
  private val openRetryWaitMs = config.getDurationMs("fg.engine.open.retry-wait")

  /** opId → engineSpace（releaseExecution 定向；有界防蔓延）。 */
  private val opBindings = new OperationBindings(10000)

  private lazy val adminHttp = java.net.http.HttpClient.newHttpClient()

  override def `type`(): String = "spark"

  override def openSession(ctx: GatewaySession): EngineSession = new SparkEngineSession(ctx)

  // ------------------------------------------------------------- 路由键

  /** 路由键计算（测试可见）：每调用按 share level 语义解析 routingUser/refId。 */
  private[client] def keyOf(s: SparkEngineSession): EngineKey = {
    val routingUser = shareLevel match {
      case "SERVER" => config.getString("fg.engine.share.server-user")
      case "GROUP" => groupProvider.primaryGroup(s.user)
      case _ => s.user
    }
    val refId = if (shareLevel == EngineKey.CONNECTION) s.ctx.sessionId() else ""
    EngineKey(EngineSpaces.engineRoot(namespace, shareLevel, "spark"),
      shareLevel, routingUser, subdomain, refId)
  }

  private def cast(session: EngineSession): SparkEngineSession =
    session.asInstanceOf[SparkEngineSession]

  // ------------------------------------------------------------- 计划/执行（ensure 漏斗）

  override def analyzeSchema(session: EngineSession, sql: String, timeout: Duration) =
    manager.ensureEngine(keyOf(cast(session))).client.analyzeSchema(session, sql, timeout)

  override def isOrderSensitive(session: EngineSession, sql: String): Boolean =
    SqlOrderHeuristics.isOrderSensitive(sql) // 纯 SQL 启发式，无引擎接触

  override def submit(session: EngineSession, sql: String, spec: MaterializationSpec,
      listener: SqlEngine.SubmitListener): CompletableFuture[EngineExecutionHandle] = {
    val me = manager.ensureEngine(keyOf(cast(session)))
    me.client.submit(session, sql, spec, bindListener(me.space, listener))
  }

  override def executeCommand(session: EngineSession, sql: String,
      listener: SqlEngine.SubmitListener): CompletableFuture[CommandOutcome] = {
    val me = manager.ensureEngine(keyOf(cast(session)))
    me.client.executeCommand(session, sql, bindListener(me.space, listener))
  }

  /** SubmitListener 包装：捕获 operationId 时记录 opId→space（release 定向用）。 */
  private def bindListener(space: String,
      listener: SqlEngine.SubmitListener): SqlEngine.SubmitListener = handle => {
    opBindings.record(handle.handle(), space)
    listener.onHandle(handle)
  }

  // ------------------------------------------------------------- attach/interrupt/release（既有引擎）

  override def attach(session: EngineSession,
      handle: EngineExecutionHandle): CompletableFuture[ExecutionOutcome] =
    manager.existingClient(keyOf(cast(session))) match {
      case Some(client) => client.attach(session, handle)
      // 引擎不在（死/未拉起）：UNKNOWN 交 manifest 对账——attach 不承载正确性
      case None => CompletableFuture.completedFuture(
        ExecutionOutcome.unknown("engine not available for attach"))
    }

  override def interrupt(session: EngineSession, handle: EngineExecutionHandle): Unit =
    manager.existingClient(keyOf(cast(session))) match {
      case Some(client) => client.interrupt(session, handle)
      case None => throw new IllegalStateException("engine not available for interrupt")
    }

  override def releaseExecution(handle: EngineExecutionHandle): Unit = {
    opBindings.consume(handle.handle()) match {
      case Some(space) =>
        manager.clientBySpace(space).foreach(_.releaseExecution(handle))
      case None =>
        // 映射缺席（网关重启/超期）：向全部在活 client 广播（幂等吞异常）
        manager.allClients.foreach(_.releaseExecution(handle))
    }
  }

  // ------------------------------------------------------------- catalog / 会话选项

  override def catalog(session: EngineSession): EngineCatalog =
    manager.ensureEngine(keyOf(cast(session))).client.catalog(session)

  override def setSessionConf(session: EngineSession, toSet: java.util.Map[String, String],
      toUnset: java.util.Set[String]): Unit =
    manager.ensureEngine(keyOf(cast(session))).client.setSessionConf(session, toSet, toUnset)

  override def getSessionConf(session: EngineSession,
      keys: java.util.Collection[String]): java.util.Map[String, String] =
    manager.ensureEngine(keyOf(cast(session))).client.getSessionConf(session, keys)

  // ------------------------------------------------------------- 会话生命周期（恢复挂点）

  override def sessionStatus(session: EngineSession): EngineSessionStatus = {
    val key = keyOf(cast(session))
    var attempt = 0
    while (true) {
      val me = manager.ensureEngine(key)
      try {
        return me.client.sessionStatus(session)
      } catch {
        case t if isConnectRefused(t) =>
          attempt += 1
          if (attempt >= openMaxAttempts) {
            throw t
          }
          manager.recover(key) // 守卫注销 + 驱逐 + ensureEngine（重拉/接管顶替者）
          Thread.sleep(openRetryWaitMs)
      }
    }
    throw new IllegalStateException("unreachable")
  }

  override def closeSession(session: EngineSession): Unit = {
    val key = keyOf(cast(session))
    manager.existingEngine(key) match {
      case Some(me) =>
        try me.client.closeSession(session)
        catch {
          case t if isConnectRefused(t) => // 引擎已死：无物可关，语义已达成
        }
        if (key.isConnection) {
          stopEngineAsync(me) // 决策 4：CONNECTION 引擎随会话下线（fire-and-forget）
          manager.evict(key)
        }
      case None => // 引擎不在（死/未拉起）：无物可关
    }
  }

  /** fire-and-forget POST /engine/stop（admin 平面；异常吞——兜底在引擎侧 checker）。 */
  private def stopEngineAsync(me: ManagedEngine): Unit = {
    val uri = java.net.URI.create(
      s"http://${me.node.host()}:${me.node.adminPort()}/engine/stop")
    try {
      executor.execute(() => {
        try {
          adminHttp.send(
            java.net.http.HttpRequest.newBuilder(uri)
              .POST(java.net.http.HttpRequest.BodyPublishers.noBody())
              .timeout(Duration.ofSeconds(5)).build(),
            java.net.http.HttpResponse.BodyHandlers.ofString())
        } catch {
          case _: Exception => // fire-and-forget
        }
      })
    } catch {
      case _: Exception => // executor 拒绝：兜底在引擎侧 checker
    }
  }

  override def close(): Unit = {
    manager.close()
    zk.close()
  }

  // ------------------------------------------------------------- connect-refused 判定

  private def isConnectRefused(t: Throwable): Boolean = {
    var cur: Throwable = t
    while (cur != null) {
      cur match {
        case _: ConnectException => return true
        case _: UnknownHostException => return true
        case _: java.net.http.HttpConnectTimeoutException => return true
        case _ =>
      }
      cur = cur.getCause
    }
    false
  }
}
