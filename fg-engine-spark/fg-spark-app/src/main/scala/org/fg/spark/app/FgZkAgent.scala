package org.fg.spark.app

import org.apache.spark.sql.SparkSession
import org.fg.ha.FgZkClient

import java.net.{InetAddress, NetworkInterface}
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters._

/**
 * 引擎侧 ZK agent（D22，Kyuubi SparkSQLEngine 生命周期范式）：
 *
 * <ul>
 *   <li><b>注册</b>：EPHEMERAL_SEQUENTIAL znode（PersistentNode：会话重连自动重建），
 *       内容 host:port，属性含 adminPort/refId/version；</li>
 *   <li><b>DeReg watch</b>：自身节点被删（网关恢复路径/运维摘流）→ 优雅停（fail-safe：
 *       臂表失败同样停——宁可误停不可僵留）；</li>
 *   <li><b>ZK LOST 宽限</b>：会话丢失超宽限未恢复 → 停（ephemeral 已消失，继续服务即
 *       脱离发现面）；</li>
 *   <li><b>max-lifetime</b>：到期摘流（注销）→ 排空 → 停；</li>
 *   <li><b>never-connected fast-fail</b>（CONNECTION 专用）：超 max-initial-wait 仍无任何
 *       Connect 会话（僵尸引擎防蔓延）；</li>
 *   <li><b>gracefulStop</b>（全退出路径汇入点）：注销 → 有界排空（FgSessionAdmin 会话登记
 *       清空）→ spark.stop → exit(0)。</li>
 * </ul>
 */
final class FgZkAgent(
    zk: FgZkClient,
    engineSpace: String,
    refId: String,
    host: String,
    connectPort: Int,
    adminPort: Int,
    isConnectionShare: Boolean,
    maxLifetimeMs: Long,
    sessionGraceMs: Long,
    maxInitialWaitMs: Long,
    drainTimeoutMs: Long,
    spark: SparkSession) {

  private val stopping = new AtomicBoolean(false)
  private val startedAt = System.currentTimeMillis()
  private var registration: FgZkClient.Registration = _

  def start(): Unit = {
    registration = zk.registerEphemeral(engineSpace, host, connectPort, adminPort, null, refId)
    zk.watchForDelete(registration.path(), () => gracefulStop("de-registered"))
    zk.onConnectionLost(sessionGraceMs, () => gracefulStop("zk-session-lost"))
    if (maxLifetimeMs > 0) startMaxLifetimeChecker(maxLifetimeMs)
    if (isConnectionShare && maxInitialWaitMs > 0) startFastFailChecker(maxInitialWaitMs)
    System.out.println(
      s"[fg-engine] registered space=$engineSpace refId=$refId uri=$host:$connectPort admin=$adminPort")
  }

  /** 全部退出路径的汇入点（idle 看门狗/admin stop/server 退出/各 checker/DeReg/LOST）。 */
  def gracefulStop(reason: String): Unit = {
    if (!stopping.compareAndSet(false, true)) {
      return
    }
    System.out.println(s"[fg-engine] graceful stop: $reason")
    // 1. 摘流：注销 znode（先于死亡——网关随即看不见本引擎）
    if (registration != null) {
      try registration.close()
      catch { case _: Throwable => }
    }
    try zk.deleteIfEmpty(engineSpace)
    catch { case _: Throwable => }
    // 2. 有界排空：等在活 Connect 会话登记清空
    val deadline = System.currentTimeMillis() + drainTimeoutMs
    while (FgSessionAdmin.activeSessionCount() > 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(500)
    }
    // 3. 停 Spark 与 ZK，退出
    try zk.close()
    catch { case _: Throwable => }
    spark.stop()
    System.exit(0)
  }

  private def startMaxLifetimeChecker(maxLifetimeMs: Long): Unit = {
    val deadline = System.currentTimeMillis() + maxLifetimeMs
    FgZkAgent.daemon("fg-engine-max-lifetime") {
      while (!Thread.currentThread().isInterrupted) {
        Thread.sleep(10 * 1000)
        if (System.currentTimeMillis() >= deadline) {
          gracefulStop("max-lifetime")
        }
      }
    }
  }

  private def startFastFailChecker(maxInitialWaitMs: Long): Unit = {
    val deadline = startedAt + maxInitialWaitMs
    FgZkAgent.daemon("fg-engine-fast-fail") {
      while (!Thread.currentThread().isInterrupted) {
        Thread.sleep(10 * 1000)
        if (System.currentTimeMillis() >= deadline && !FgSessionAdmin.hasEverConnected()) {
          gracefulStop("never-connected")
        }
      }
    }
  }
}

private[app] object FgZkAgent {

  /** daemon 线程包装（checker 用）。 */
  def daemon(name: String)(body: => Unit): Thread = {
    val t = new Thread(() => body, name)
    t.setDaemon(true)
    t.start()
    t
  }

  /** 本机非回环 IPv4 地址（注册进 znode 的可达 host；兜底 getLocalHost）。 */
  def localHostAddress(): String = {
    try {
      val it = NetworkInterface.getNetworkInterfaces()
      while (it.hasMoreElements) {
        val addrs = it.nextElement().getInetAddresses.asScala
        addrs.foreach {
          case a: java.net.Inet4Address if !a.isLoopbackAddress => return a.getHostAddress
          case _ =>
        }
      }
    } catch {
      case _: Throwable => // 兜底
    }
    InetAddress.getLocalHost.getHostAddress
  }
}
