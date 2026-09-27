package org.fg.spark.client

import org.apache.spark.launcher.SparkAppHandle
import org.apache.spark.launcher.SparkLauncher
import org.fg.common.config.GatewayConfig

import java.io.File
import scala.jdk.CollectionConverters._

/**
 * 引擎拉起器（D22）——Kit 内唯一懂 deploy-mode 的类。SparkLauncher（纯 Java 瘦件，
 * 无 spark-core 传递）spawn spark-submit 子进程。
 *
 * <p>引擎配置经 {@code spark.fg.*} conf 下发（决策 6：系统属性过不了 spark-submit JVM
 * 边界）：zk 发现参数、engine space/refId/端口、生命周期时限、result s3 凭证；
 * {@code fg.engine.spark.launch.conf.*} 透传任意 spark conf。
 *
 * <p>local 模式（master=local[*]、deploy-mode=client）：驱动进程即引擎进程，端口经
 * 端口段分配；cluster 模式：固定配置端口、launcher 进程早退不算失败（就绪只看 ZK 注册）。
 */
private[client] final class EngineLauncher(config: GatewayConfig) {

  def launch(key: EngineKey, engineSpace: String, refId: String, connectPort: Int,
      adminPort: Int): LaunchedEngine = {
    val master = config.getString("fg.engine.spark.launch.master")
    val deployMode = config.getString("fg.engine.spark.launch.deploy-mode")
    val sparkHome = config.getString("fg.engine.spark.launch.home").trim
    val appJar = config.getString("fg.engine.spark.launch.app-jar").trim
    if (sparkHome.isEmpty) {
      throw new IllegalStateException("fg.engine.spark.launch.home required for engine launch")
    }
    if (appJar.isEmpty || !new File(appJar).isFile) {
      throw new IllegalStateException(s"fg.engine.spark.launch.app-jar missing or not a file: $appJar")
    }

    val appName = s"fg_${key.shareLevel}_${key.routingUser}" +
      (if (key.refId != null && key.refId.nonEmpty) s"_${key.refId.take(8)}" else "")

    val launcher = new SparkLauncher()
      .setSparkHome(sparkHome)
      .setAppResource(appJar)
      .setMainClass("org.fg.spark.app.FgEngineMain")
      .setAppName(appName)
      .setMaster(master)
      .setDeployMode(deployMode)

    // 发现面参数（引擎侧 FgZkAgent 消费）
    launcher.setConf("spark.fg.zk.addresses", config.getString("fg.zk.addresses"))
    launcher.setConf("spark.fg.zk.namespace", config.getString("fg.zk.namespace"))
    launcher.setConf("spark.fg.engine.space", engineSpace)
    launcher.setConf("spark.fg.engine.ref.id", refId)
    launcher.setConf("spark.fg.engine.share.level", key.shareLevel)
    launcher.setConf("spark.fg.engine.connect.port", connectPort.toString)
    launcher.setConf("spark.fg.engine.admin.port", adminPort.toString)

    // 生命周期时限（毫秒数值——引擎侧 durationProp 直读）
    launcher.setConf("spark.fg.engine.idle.timeout", config.getDurationMs("fg.engine.idle.timeout").toString)
    launcher.setConf("spark.fg.engine.max-lifetime", config.getDurationMs("fg.engine.max-lifetime").toString)
    launcher.setConf("spark.fg.engine.zk.session-grace", config.getDurationMs("fg.engine.zk.session-grace").toString)
    launcher.setConf("spark.fg.engine.max-initial-wait", config.getDurationMs("fg.engine.max-initial-wait").toString)

    // result s3 凭证下发（网关配置 → 引擎 s3a）
    if (config.hasPath("fg.result.s3.endpoint")) {
      launcher.setConf("spark.fg.result.s3.endpoint", config.getString("fg.result.s3.endpoint"))
    }
    if (config.hasPath("fg.result.s3.access-key")) {
      launcher.setConf("spark.fg.result.s3.access-key", config.getString("fg.result.s3.access-key"))
    }
    if (config.hasPath("fg.result.s3.secret-key")) {
      launcher.setConf("spark.fg.result.s3.secret-key", config.getString("fg.result.s3.secret-key"))
    }

    // 驱动资源（local 模式即引擎进程内存）
    launcher.setConf("spark.driver.memory", config.getString("fg.engine.spark.launch.driver-memory"))
    // Connect 绑定端口（SparkConnectService 读 SparkEnv.conf 的 static conf——键名是
    // spark.connect.grpc.binding.port，builder.config("spark.connect.grpc.port") 无效）
    launcher.setConf("spark.connect.grpc.binding.port", connectPort.toString)

    // extra jars（cluster 模式携带 sink 等）
    config.getStringList("fg.engine.spark.launch.extra-jars").asScala
      .map(_.trim).filter(_.nonEmpty).foreach(launcher.addJar)

    // 任意 spark conf 透传
    config.getFlatEntries("fg.engine.spark.launch.conf").forEach { (k, v) =>
      launcher.setConf(k, v)
    }

    // stdout/err 重定向
    val logDir = expandHome(config.getString("fg.engine.spark.launch.log-dir"))
    new File(logDir).mkdirs()
    launcher.redirectOutput(new File(logDir, s"$refId.log"))
    launcher.redirectError(new File(logDir, s"$refId.err.log"))

    new LaunchedEngine(launcher.startApplication(), deployMode)
  }

  private def expandHome(path: String): String =
    if (path.startsWith("~")) System.getProperty("user.home") + path.substring(1) else path
}

/**
 * 拉起产物：client 模式下 FAILED/KILLED/LOST/FINISHED（main 返回=引擎死）即异常退出；
 * cluster 模式 spark-submit 早退是正常态（就绪只看 ZK 注册），状态不作失败信号。
 */
private[client] final class LaunchedEngine(handle: SparkAppHandle, deployMode: String) {

  def exitedAbnormally: Boolean = {
    if (deployMode.equalsIgnoreCase("cluster")) {
      return false
    }
    import SparkAppHandle.State._
    handle.getState match {
      case FAILED | KILLED | LOST | FINISHED => true
      case _ => false
    }
  }

  def destroy(): Unit = {
    try handle.kill()
    catch { case _: Exception => }
  }
}
