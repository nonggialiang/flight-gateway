package org.fg.spark.client

import java.net.ServerSocket
import scala.util.Random

/**
 * local 拉起模式的端口段分配器（D22 决策 8）：bind 试探后立即释放，返回可用端口。
 * 段内随机起点 + 环形扫描；全段耗尽抛异常。引擎侧 connect 端口有 preflight 兜底，
 * 此处的试探窗口竞争（分配后引擎 bind 前）只影响冷启动成功率，不影响正确性。
 */
object PortRangeAllocator {

  /** "16000-16999" → (16000, 16999)；单端口 "16000" → (16000, 16000)。 */
  def parseRange(range: String): (Int, Int) = {
    val r = range.trim
    val i = r.indexOf('-')
    if (i < 0) {
      val p = r.toInt
      (p, p)
    } else {
      (r.substring(0, i).trim.toInt, r.substring(i + 1).trim.toInt)
    }
  }

  /** 在段内分配一个当前可 bind 的端口。 */
  def allocate(range: String): Int = {
    val (lo, hi) = parseRange(range)
    require(hi >= lo, s"invalid port range: $range")
    val span = hi - lo + 1
    val start = lo + Random.nextInt(span)
    var tried = 0
    var port = start
    while (tried < span) {
      if (bindable(port)) return port
      port = if (port == hi) lo else port + 1
      tried += 1
    }
    throw new IllegalStateException(s"no free port in range $range")
  }

  private def bindable(port: Int): Boolean = {
    var socket: ServerSocket = null
    try {
      socket = new ServerSocket(port)
      true
    } catch {
      case _: Exception => false
    } finally {
      if (socket != null) try socket.close() catch { case _: Exception => }
    }
  }
}
