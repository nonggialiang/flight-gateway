package org.fg.spark.app

import com.sun.net.httpserver.HttpServer
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.connect.service.{
  SessionHolder, SparkConnectService, SparkListenerConnectSessionClosed, SparkListenerConnectSessionStarted
}

import java.net.{InetSocketAddress, URLDecoder}
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

/**
 * Spark Connect 会话生命周期管理面（design D20 生命周期绑定，gateway 专属 admin 通道）。
 *
 * <p>背景（协议事实，Spark 3.5.9）：Connect 服务只有 8 个 rpc（ExecutePlan/AnalyzePlan/
 * Config/AddArtifacts/ArtifactStatus/Interrupt/ReattachExecute/ReleaseExecute）——
 * <b>没有</b>会话关闭 RPC（Spark 4.0 才有 SPARK-45680 ReleaseSession），且收到未知
 * session_id 的请求会<b>静默重建</b>同 id 新会话（`SparkConnectService$.userSessionMapping`
 * Guava cache，带 LRU/idle 逐出）。因此：
 *
 * <ul>
 *   <li><b>status</b>：经 listener bus 的
 *       {@link SparkListenerConnectSessionStarted}/{@link SparkListenerConnectSessionClosed}
 *       事件自建登记（(userId, sessionId) → eventTime）——eventTime 即化身标记，引擎重启后
 *       同 id 重建会产生新 eventTime，网关比对即可检出"转世"；且查询无副作用（不经
 *       Connect RPC 就不会误建会话）；</li>
 *   <li><b>close</b>：反射读 `userSessionMapping` cache 后 invalidate(key)——removal
 *       listener（{@code SparkConnectService.RemoveSessionListener}）会调用
 *       {@code SessionHolder.expireSession()} 做正规清理（artifact 释放 + streaming query
 *       清理 + Closed 事件），与 idle 逐出同一条路径。升级 Spark 4.x 后可换原生
 *       ReleaseSession RPC，gateway SPI 不变。</li>
 * </ul>
 *
 * <p>传输 = 引擎本机 HTTP/JSON（`fg.engine.admin.port`，默认 15003）：两个动作
 * `GET /session/status?user&id`、`POST /session/close?user&id`。信任域内使用（与 Connect
 * 端口同级暴露），M1.5 无鉴权。
 */
private[app] object FgSessionAdmin {

  /** 化身登记：(userId, sessionId) → 会话创建 eventTime（ms）。引擎重启即清空 → 全量 engine_lost。 */
  private val startedAt = new ConcurrentHashMap[String, java.lang.Long]()

  private def key(user: String, sessionId: String): String = user + "\u0000" + sessionId

  /** 注册会话生命周期监听（FgEngineMain 启动时调用一次）。 */
  def registerListener(spark: org.apache.spark.sql.SparkSession): Unit =
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case e: SparkListenerConnectSessionStarted =>
          startedAt.put(key(e.userId, e.sessionId), e.eventTime)
        case e: SparkListenerConnectSessionClosed =>
          startedAt.remove(key(e.userId, e.sessionId))
        case _ =>
      }
    })

  /** 会话状态：alive + 化身起点（eventTime ms）；未登记即 not alive。 */
  def status(user: String, sessionId: String): (Boolean, Long) = {
    val t = startedAt.get(key(user, sessionId))
    if (t == null) (false, 0L) else (true, t.longValue())
  }

  /**
   * 关闭会话：cache invalidate 触发正规清理路径（removal listener → expireSession）。
   * 返回 false = 会话本不在 cache（已逐出/重启后未建），无物可关。
   */
  def close(user: String, sessionId: String): Boolean = {
    val cache = sessionCache()
    val cacheKey = new scala.Tuple2(user, sessionId)
    if (cache.getIfPresent(cacheKey) == null) {
      false
    } else {
      cache.invalidate(cacheKey)
      true
    }
  }

  /** 反射取 SparkConnectService$.userSessionMapping（private 静态字段，同 JVM 内可达）。 */
  private def sessionCache(): org.sparkproject.connect.guava.cache.Cache[scala.Tuple2[String, String], SessionHolder] = {
    val moduleClass = SparkConnectService.getClass // class org.apache.spark.sql.connect.service.SparkConnectService$
    val field = moduleClass.getDeclaredField("userSessionMapping")
    field.setAccessible(true)
    field.get(null).asInstanceOf[org.sparkproject.connect.guava.cache.Cache[scala.Tuple2[String, String], SessionHolder]]
  }

  // ------------------------------------------------------------- HTTP 面向 gateway

  def startHttp(port: Int): Unit = {
    val server = HttpServer.create(new InetSocketAddress(port), 0)
    server.createContext("/session/status", exchange => {
      val (user, sessionId) = params(exchange.getRequestURI.getRawQuery)
      val (alive, started) = status(user, sessionId)
      respond(exchange, 200, s"""{"alive":$alive,"startedAt":$started}""")
    })
    server.createContext("/session/close", exchange => {
      val (user, sessionId) = params(exchange.getRequestURI.getRawQuery)
      val closed = close(user, sessionId)
      respond(exchange, if (closed) 200 else 404, s"""{"closed":$closed}""")
    })
    server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(2, r => {
      val t = new Thread(r, "fg-engine-session-admin")
      t.setDaemon(true)
      t
    }))
    server.start()
    System.out.println(s"[fg-engine] session admin listening on $port")
  }

  private def params(rawQuery: String): (String, String) = {
    val m = new scala.collection.mutable.HashMap[String, String]()
    if (rawQuery != null) {
      rawQuery.split("&").filter(_.contains("=")).foreach { kv =>
        val i = kv.indexOf('=')
        m.put(URLDecoder.decode(kv.substring(0, i), StandardCharsets.UTF_8),
          URLDecoder.decode(kv.substring(i + 1), StandardCharsets.UTF_8))
      }
    }
    (m.getOrElse("user", ""), m.getOrElse("id", ""))
  }

  private def respond(exchange: com.sun.net.httpserver.HttpExchange, code: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.set("Content-Type", "application/json")
    exchange.sendResponseHeaders(code, bytes.length)
    exchange.getResponseBody.write(bytes)
    exchange.close()
  }
}
