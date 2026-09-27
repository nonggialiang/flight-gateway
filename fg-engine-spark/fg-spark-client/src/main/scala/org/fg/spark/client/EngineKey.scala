package org.fg.spark.client

import org.fg.ha.EngineSpaces

/**
 * 引擎路由键（D22）：每次调用按 (shareLevel, routingUser, subdomain[, refId]) 纯函数重算。
 * space 即 ZK engine space；CONNECTION 的 refId = fg 会话 id（space 内嵌，UUID 永不复用）。
 */
final case class EngineKey(
    root: String,
    shareLevel: String,
    routingUser: String,
    subdomain: String,
    refId: String) {

  require(root != null && root.startsWith("/"), "root required")
  require(routingUser != null && routingUser.nonEmpty, "routingUser required")

  def isConnection: Boolean = EngineKey.CONNECTION.equalsIgnoreCase(shareLevel)

  /** ZK engine space（fg-ha 契约）。 */
  def space: String = {
    val base = EngineSpaces.engineSpace(root, routingUser, subdomain)
    if (refId != null && refId.nonEmpty) EngineSpaces.connectionSpace(base, refId) else base
  }

  /** 冷启动互斥锁路径（CONNECTION 免锁——space 内嵌唯一 refId）。 */
  def lockPath: String = EngineSpaces.lockPath(root, routingUser, subdomain)
}

object EngineKey {
  val CONNECTION: String = "CONNECTION"
}
