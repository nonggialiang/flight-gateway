package org.fg.spark.app

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.service.SparkConnectServer

import java.util.concurrent.atomic.AtomicLong

/**
 * 薄启动壳（design §4.4.1，路线 B）：
 *   1. fg-result DSv2 经 ServiceLoader 自动加载（jar 在 classpath 即注册）
 *   2. vanilla SparkConnectServer 承载执行/会话/取消/健康/元数据
 *   3. idle timeout 自杀钩子（M2 增加 ZK agent 注册/宽限/首连 fast-fail）
 *
 * s3a 静态 scoped 凭证经系统属性注入（引擎级配置，D5），此处转写 spark conf。
 */
object FgEngineMain {

  private def prop(key: String): Option[String] = Option(System.getProperty(key))

  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder().appName("fg-spark-engine")

    // s3a 静态 scoped 凭证（D5）：launcher 注入系统属性 → spark conf
    prop("fg.result.s3.endpoint").foreach(builder.config("fs.s3a.endpoint", _))
    prop("fg.result.s3.access-key").foreach(builder.config("fs.s3a.access.key", _))
    prop("fg.result.s3.secret-key").foreach(builder.config("fs.s3a.secret.key", _))
    builder.config("fs.s3a.path.style.access", "true")
    builder.config("fs.s3a.connection.ssl.enabled",
      prop("fg.result.s3.endpoint").exists(_.startsWith("https")))
    // 避免结果小对象走 multipart
    builder.config("fs.s3a.multipart.size", "32M")

    // Connect server 端口与 reattachable 执行（⑤ 主路径依赖）
    builder.config("spark.connect.grpc.port",
      prop("fg.engine.connect.port").getOrElse("15002"))
    builder.config("spark.sql.connect.execute.reattachable.enabled", "true")

    val spark = builder.getOrCreate()

    // idle 自杀钩子：fg.engine.idle.timeout（默认 30m）无作业活动即退出（M1 简化为作业级判定）
    val lastActivity = new AtomicLong(System.currentTimeMillis())
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit =
        lastActivity.set(System.currentTimeMillis())
      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit =
        lastActivity.set(System.currentTimeMillis())
    })
    val idleTimeoutMs = prop("fg.engine.idle.timeout")
      .map(java.time.Duration.parse(_).toMillis)
      .getOrElse(30L * 60 * 1000)
    val watchdog = new Thread(() => {
      while (!Thread.currentThread().isInterrupted) {
        Thread.sleep(30 * 1000)
        if (System.currentTimeMillis() - lastActivity.get() > idleTimeoutMs
            && spark.sparkContext.statusTracker.getJobIdsForGroup(null).isEmpty) {
          System.out.println(s"[fg-engine] idle timeout (${idleTimeoutMs}ms) reached, exiting")
          spark.stop()
          System.exit(0)
        }
      }
    }, "fg-engine-idle-watchdog")
    watchdog.setDaemon(true)
    watchdog.start()

    // vanilla Connect server（阻塞服务）
    SparkConnectServer.main(Array.empty)
  }
}
