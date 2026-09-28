package org.fg.spark.app

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.service.SparkConnectServer
import org.fg.ha.FgZkClient

import java.net.{InetSocketAddress, ServerSocket}
import java.util.concurrent.atomic.AtomicLong

/**
 * 薄启动壳（design §4.4.1，路线 B）：
 *   1. fg-result DSv2 经 ServiceLoader 自动加载（jar 在 classpath 即注册）
 *   2. vanilla SparkConnectServer 承载执行/会话/取消/健康/元数据
 *   3. 生命周期钩子：idle timeout 看门狗 + ZK agent（D22：注册/宽限/首连 fast-fail/max-lifetime）
 *
 * <p>s3a 静态 scoped 凭证与引擎参数经 spark conf 下发（D22 决策 6：系统属性过不了
 * spark-submit JVM 边界）——读取器 {@link #sparkProp} conf 优先（spark-submit --conf 在
 * driver JVM 落 spark.* 系统属性）、手工 {@code -D} 启动方式兜底兼容。
 *
 * <p>所有退出路径（idle 看门狗/admin /engine/stop/server 退出/checker）统一汇入
 * gracefulStop——ZK 模式下注销先于死亡（D20/D22 语义）。
 */
object FgEngineMain {

  /** conf 优先读取器：spark.fg.* conf（--conf 下发）→ fg.* 系统属性（手工 -D）。 */
  private def sparkProp(key: String): Option[String] =
    Option(System.getProperty("spark." + key)).orElse(Option(System.getProperty(key)))

  /** 时长读取器：毫秒数值（launcher 下发）优先，回落 ISO-8601/simple（手工 -D）。 */
  private def durationProp(key: String, defaultMs: Long): Long =
    sparkProp(key).map(FgEngineMain.parseDuration).getOrElse(defaultMs)

  def main(args: Array[String]): Unit = {
    val connectPort = sparkProp("fg.engine.connect.port").map(_.trim.toInt).getOrElse(15002)
    val adminPort = sparkProp("fg.engine.admin.port").map(_.trim.toInt).getOrElse(15003)

    // connect 端口 preflight（bind+close 快败）：先于 Spark 会话/ZK 注册，冲突即非零退出
    preflightPort(connectPort)

    val builder = SparkSession.builder().appName("fg-spark-engine")

    // s3a 静态 scoped 凭证（D5）：launcher --conf / 手工 -D → spark conf
    sparkProp("fg.result.s3.endpoint").foreach(builder.config("spark.hadoop.fs.s3a.endpoint", _))
    sparkProp("fg.result.s3.access-key").foreach(builder.config("spark.hadoop.fs.s3a.access.key", _))
    sparkProp("fg.result.s3.secret-key").foreach(builder.config("spark.hadoop.fs.s3a.secret.key", _))
    builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled",
      sparkProp("fg.result.s3.endpoint").exists(_.startsWith("https")))
    // 避免结果小对象走 multipart
    builder.config("spark.hadoop.fs.s3a.multipart.size", "32M")

    // Connect server 端口与 reattachable 执行（⑤ 主路径依赖）。
    // 端口键 = spark.connect.grpc.binding.port（SparkConnectService 读 SparkEnv.conf 的
    // static conf）；手工 -D 模式兜底仍经 builder 传入 SparkConf。
    builder.config("spark.connect.grpc.binding.port", connectPort.toString)
    builder.config("spark.sql.connect.execute.reattachable.enabled", "true")

    val spark = builder.getOrCreate()

    // ZK agent（D22）：地址非空即启用发现面 + 生命周期 checker
    var agent: Option[FgZkAgent] = None
    sparkProp("fg.zk.addresses").map(_.trim).filter(_.nonEmpty).foreach { addrs =>
      val zk = new FgZkClient(addrs, sparkProp("fg.zk.namespace").getOrElse("flight-gateway"))
      zk.start()
      val a = new FgZkAgent(
        zk = zk,
        engineSpace = sparkProp("fg.engine.space").getOrElse(""),
        refId = sparkProp("fg.engine.ref.id").getOrElse(java.util.UUID.randomUUID().toString),
        host = FgZkAgent.localHostAddress(),
        connectPort = connectPort,
        adminPort = adminPort,
        isConnectionShare = sparkProp("fg.engine.share.level").exists(_.equalsIgnoreCase("CONNECTION")),
        maxLifetimeMs = durationProp("fg.engine.max-lifetime", 0L),
        sessionGraceMs = durationProp("fg.engine.zk.session-grace", 30000L),
        maxInitialWaitMs = durationProp("fg.engine.max-initial-wait", 10L * 60 * 1000),
        drainTimeoutMs = 60000L,
        spark = spark)
      a.start()
      agent = Some(a)
    }

    // 统一停机路径：ZK 模式走 agent（注销→排空→spark.stop→exit）；固定模式保持 M1 行为
    def stopEngine(reason: String): Unit = agent match {
      case Some(a) => a.gracefulStop(reason)
      case None =>
        System.out.println(s"[fg-engine] stop: $reason")
        spark.stop()
        System.exit(0)
    }

    // 会话生命周期管理面（D20）：listener 登记会话化身 + HTTP admin（status/close/engine-stop）
    FgSessionAdmin.registerListener(spark)
    FgSessionAdmin.startHttp(adminPort, Some(() => stopEngine("admin-stop")))

    // idle 自杀钩子：fg.engine.idle.timeout（默认 30m）无作业活动即退出（M1 简化为作业级判定）
    val lastActivity = new AtomicLong(System.currentTimeMillis())
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit =
        lastActivity.set(System.currentTimeMillis())
      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit =
        lastActivity.set(System.currentTimeMillis())
    })
    val idleTimeoutMs = durationProp("fg.engine.idle.timeout", 30L * 60 * 1000)
    val watchdog = new Thread(() => {
      try {
        while (!Thread.currentThread().isInterrupted) {
          Thread.sleep(30 * 1000)
          if (System.currentTimeMillis() - lastActivity.get() > idleTimeoutMs
              && spark.sparkContext.statusTracker.getJobIdsForGroup(null).isEmpty) {
            stopEngine(s"idle timeout (${idleTimeoutMs}ms) reached")
          }
        }
      } catch {
        case _: InterruptedException => // 停机中
      }
    }, "fg-engine-idle-watchdog")
    watchdog.setDaemon(true)
    watchdog.start()

    // vanilla Connect server（阻塞服务；返回即 server 已终——收尾走统一停机）
    SparkConnectServer.main(Array.empty)
    stopEngine("connect-server-exited")
  }

  /** 端口可用性预检：bind 后立即 close；占用即抛 BindException → 进程非零退出。 */
  private def preflightPort(port: Int): Unit = {
    val socket = new ServerSocket()
    try socket.bind(new InetSocketAddress(port))
    finally socket.close()
    System.out.println(s"[fg-engine] connect port preflight ok: $port")
  }

  /** 时长解析：纯数值=ms；否则 ISO-8601（PT30M）或 simple（30m/600s/1h/2d）。 */
  def parseDuration(v: String): Long = {
    val s = v.trim
    if (s.matches("\\d+")) return s.toLong
    try {
      java.time.Duration.parse(s).toMillis // ISO-8601（含 PT 前缀负形式）
    } catch {
      case _: Exception =>
        val m = "^(\\d+)(ms|s|m|h|d)$".r.findFirstMatchIn(s.toLowerCase)
        m.map { x =>
          val n = x.group(1).toLong
          x.group(2) match {
            case "ms" => n
            case "s" => n * 1000
            case "m" => n * 60 * 1000
            case "h" => n * 3600 * 1000
            case "d" => n * 24 * 3600 * 1000
          }
        }.getOrElse(throw new IllegalArgumentException(s"unparseable duration: $v"))
    }
  }
}
