package org.fg.spark.client

import com.google.common.cache.CacheBuilder

import java.util.concurrent.TimeUnit

/**
 * opId → engineSpace 有界映射（D22 决策 2）：submit/executeCommand 捕获 operationId 时
 * record，releaseExecution 时 consume（一次性）。缺席（网关重启/超期/内存上界逐出）由
 * 调用方回落全 client 广播——release 幂等，广播无害。
 *
 * <p>与 fg_operation.connect_operation_id 的持久事实互补：DB 行恢复的是 handle 本身，
 * 此映射只做"哪个引擎持有该执行"的网关内存态定向。
 */
private[client] final class OperationBindings(maximumSize: Long) {

  private val cache = CacheBuilder.newBuilder()
    .maximumSize(maximumSize)
    .expireAfterWrite(1, TimeUnit.HOURS)
    .build[String, String]()

  def record(opId: String, space: String): Unit = cache.put(opId, space)

  /** 消费式读取：命中即移除（release 义务一次性）。 */
  def consume(opId: String): Option[String] = {
    val space = cache.getIfPresent(opId)
    if (space != null) cache.invalidate(opId)
    Option(space)
  }
}
