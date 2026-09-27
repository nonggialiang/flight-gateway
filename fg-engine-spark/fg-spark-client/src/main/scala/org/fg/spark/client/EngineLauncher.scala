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
 * 端口段分配；cluster 模式：固定配置端口、driver JVM 由 worker 拉起（注入 JDK17
 * add-opens），FAILED/KILLED/LOST 快败、FINISHED 容忍，就绪判定只看 ZK 注册。
 */
private[client] object EngineLauncher {
  /** JDK17+ driver 必需的模块开放（Spark 3.5 JavaModuleOptions 同款子集：netty/arrow/
   * StorageUtils/codegen 触达面）。cluster 模式注入 spark.driver.extraJavaOptions。 */
  val Jdk17AddOpens: String =
    "--add-opens=java.base/java.lang=ALL-UNNAMED " +
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED " +
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
      "--add-opens=java.base/java.io=ALL-UNNAMED " +
      "--add-opens=java.base/java.net=ALL-UNNAMED " +
      "--add-opens=java.base/java.nio=ALL-UNNAMED " +
      "--add-opens=java.base/java.util=ALL-UNNAMED " +
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED " +
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED " +
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED " +
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED " +
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED " +
      "--add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED " +
      "--add-opens=jdk.unsupported/sun.reflect=ALL-UNNAMED"
}

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
    val confPassthrough = config.getFlatEntries("fg.engine.spark.launch.conf")
    confPassthrough.forEach { (k, v) =>
      launcher.setConf(k, v)
    }

    // cluster 模式：worker 拉起的 driver JVM 不带 add-opens（client 模式由 spark-submit
    // 注入）——JDK17+ 无之则 SparkContext init 即 IllegalAccessError（StorageUtils→
    // DirectBuffer，standalone 实证）。与 launch.conf 传入的 extraJavaOptions 合并
    //（add-opens 叠加无冲突）。
    if (deployMode.equalsIgnoreCase("cluster")) {
      val user = confPassthrough.get("spark.driver.extraJavaOptions")
      val merged = (if (user != null && user.nonEmpty) user + " " else "") + EngineLauncher.Jdk17AddOpens
      launcher.setConf("spark.driver.extraJavaOptions", merged)
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
 * 拉起产物。client 模式：FAILED/KILLED/LOST/FINISHED（main 返回=引擎死）即异常退出。
 * cluster 模式：FAILED/KILLED/LOST 仍为失败信号（submitter 轮询 master，driver 终态
 * 失败时上报后退出——不傻等 ZK 超时）；FINISHED 容忍（submitter 可在 driver 起跑后
 * 早退，或 driver 完成后正常退出），就绪判定只看 ZK 注册。destroy 只及 submitter
 * （真实集群引擎无进程句柄，超时清理由引擎侧 idle/max-lifetime 兜底）。
 */
private[client] final class LaunchedEngine(handle: SparkAppHandle, deployMode: String) {

  def exitedAbnormally: Boolean = {
    import SparkAppHandle.State._
    handle.getState match {
      case FAILED | KILLED | LOST => true
      case FINISHED => !deployMode.equalsIgnoreCase("cluster")
      case _ => false
    }
  }

  def destroy(): Unit = {
    try handle.kill()
    catch { case _: Exception => }
  }
}
