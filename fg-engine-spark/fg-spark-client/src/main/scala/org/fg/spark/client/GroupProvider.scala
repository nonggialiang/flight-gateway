package org.fg.spark.client

import org.fg.common.config.GatewayConfig
import scala.jdk.CollectionConverters._

/**
 * GROUP share level 的路由用户解析（D22，Kyuubi GroupProvider 范式的最小内嵌版）。
 * Kit 内 SPI——后续可扩展为外部组服务（LDAP/文件）实现，本阶段只交付静态配置映射。
 */
trait GroupProvider {
  def primaryGroup(user: String): String
}

/**
 * 静态配置映射：`fg.engine.share.group-mapping`（"user:group" 列表或逗号串）。
 * 未映射用户回落 user 本身（文档化行为——语义上退化为 USER share level）。
 */
final class StaticGroupProvider(config: GatewayConfig) extends GroupProvider {

  private val mapping: Map[String, String] =
    config.getStringList("fg.engine.share.group-mapping").asScala
      .map(_.trim)
      .filter(_.nonEmpty)
      .flatMap { entry =>
        val i = entry.indexOf(':')
        if (i <= 0) None else Some(entry.substring(0, i).trim -> entry.substring(i + 1).trim)
      }
      .toMap

  override def primaryGroup(user: String): String = mapping.getOrElse(user, user)
}
