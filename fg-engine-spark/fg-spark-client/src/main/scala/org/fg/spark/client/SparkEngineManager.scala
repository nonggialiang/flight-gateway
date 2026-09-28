package org.fg.spark.client

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.fg.common.config.GatewayConfig
import org.fg.ha.EngineNode
import org.fg.ha.FgZkClient
import org.slf4j.LoggerFactory

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

/**
 * 引擎管理器（D22，Kyuubi EngineRef 三段式）：
 *
 * <ol>
 *   <li>查 children（读穿发现，无 watcher 缓存——CONNECTION 使 space 数无界，见 fg-ha）；
 *       命中即缓存复用；</li>
 *   <li>miss → ZK 锁（engineSpace 同级 {@code _lock} 路径）+ 锁内 double-check。
 *       CONNECTION 的锁粒度为<strong>用户级</strong>（lockPath 不含 sessionId 段——逐会话
 *       锁路径是持久 znode，会话数无界即泄漏；用户级有界，且足以去重多实例对同一会话
 *       space 的竞争：先行者注册后，后来者锁内 double-check 命中即复用）。代价：同用户
 *       不同会话的并发冷启动在锁上串行（不同用户仍并行，全局另有 launch semaphore）；</li>
 *   <li>拉起（launch semaphore 限并发）→ 按 refId 1s 轮询 ZK 至 initialize.timeout
 *       （local 模式进程退出≠0 立即失败；超时 destroy）→ SparkEngineClient 入缓存。</li>
 * </ol>
 *
 * <p>进程内 per-space 门闩（launching map）使同引擎并发冷启动收敛为一次等待。
 * {@link #recover}（sessionStatus connect-refused 时由 router 调用）：守卫式注销陈旧
 * znode + 驱逐旧 client + 重新 ensureEngine。housekeeping 定期清点缓存——znode 已消失
 * （引擎死/会话过期）的缓存项即驱逐，下一 RPC 冷启动自愈。
 */
private[client] class SparkEngineManager(
    config: GatewayConfig,
    zk: FgZkClient,
    meterRegistry: MeterRegistry) extends AutoCloseable {

  private val log = LoggerFactory.getLogger(getClass)

  private val initTimeoutMs = config.getDurationMs("fg.engine.initialize.timeout")
  private val lockTimeoutMs = (initTimeoutMs * 1.1).toLong
  private val maxConcurrentLaunches = config.getInt("fg.engine.launch.max.concurrent")
  private val isClusterMode = config.getString("fg.engine.spark.launch.deploy-mode").equalsIgnoreCase("cluster")

  private val engines = new ConcurrentHashMap[String, ManagedEngine]()
  private val launching = new ConcurrentHashMap[String, CompletableFuture[ManagedEngine]]()
  private val launchSlots = new Semaphore(maxConcurrentLaunches)
  private val launcher = new EngineLauncher(config)

  private val launchExecutor: ExecutorService = Executors.newCachedThreadPool(r => {
    val t = new Thread(r, "fg-engine-launch")
    t.setDaemon(true)
    t
  })
  private val sweeper: ScheduledExecutorService = Executors.newScheduledThreadPool(1, r => {
    val t = new Thread(r, "fg-engine-sweep")
    t.setDaemon(true)
    t
  })
  sweeper.scheduleWithFixedDelay(() => evictDeadEngines(), 30, 30, TimeUnit.SECONDS)

  Gauge.builder("fg.engine.live", engines, (m: ConcurrentHashMap[String, ManagedEngine]) => m.size().toDouble)
    .register(meterRegistry)
  Gauge.builder("fg.engine.launch.concurrent", launchSlots,
    (s: Semaphore) => (maxConcurrentLaunches - s.availablePermits()).toDouble)
    .register(meterRegistry)
  private val launchesCounter: Counter = Counter.builder("fg.engine.launches").register(meterRegistry)
  private val launchFailuresCounter: Counter =
    Counter.builder("fg.engine.launch.failures").register(meterRegistry)
  private val recoveriesCounter: Counter =
    Counter.builder("fg.engine.recoveries").register(meterRegistry)
  private def evictionsCounter(reason: String): Counter =
    meterRegistry.counter("fg.engine.evictions", "reason", reason)

  // ------------------------------------------------------------- 发现与拉起

  /** 所有流量的漏斗：缓存 → ZK 读穿 → （miss）锁内 double-check 拉起。 */
  def ensureEngine(key: EngineKey): ManagedEngine = {
    val space = key.space
    val cached = engines.get(space)
    if (cached != null) return cached
    discover(space).foreach(node => return cache(space, node))
    followLaunch(space, launchUnderLatch(key))
  }

  /** 既有引擎（不拉起）：closeSession/attach/interrupt 用——死引擎上不做无谓冷启动。 */
  def existingEngine(key: EngineKey): Option[ManagedEngine] = {
    val space = key.space
    val cached = engines.get(space)
    if (cached != null) Some(cached)
    else discover(space).map(node => cache(space, node))
  }

  def existingClient(key: EngineKey): Option[SparkEngineClient] =
    existingEngine(key).map(_.client)

  /**
   * 恢复（仅 sessionStatus 的 connect-refused 路径调用）：守卫式注销陈旧 znode +
   * 驱逐旧 client + ensureEngine（重拉或接管顶替者）。
   */
  def recover(key: EngineKey): ManagedEngine =
    followLaunch(key.space, {
      val space = key.space
      Option(engines.remove(space)).foreach { me =>
        recoveriesCounter.increment()
        log.info(s"recovering engine space=$space stale=${me.node}")
        try {
          zk.deregisterIfStale(space, key.lockPath,
            me.node.host(), me.node.connectPort(), lockTimeoutMs)
        } finally {
          closeQuietly(me.client)
          evictionsCounter("recover").increment()
        }
      }
      discoverOrLaunch(key)
    })

  def clientBySpace(space: String): Option[SparkEngineClient] =
    Option(engines.get(space)).map(_.client)

  def allClients: Seq[SparkEngineClient] = engines.values().asScala.map(_.client).toSeq

  /** 显式驱逐（CONNECTION closeSession 用）。 */
  def evict(key: EngineKey): Unit = {
    val me = engines.remove(key.space)
    if (me != null) {
      closeQuietly(me.client)
      evictionsCounter("close").increment()
    }
  }

  override def close(): Unit = {
    sweeper.shutdownNow()
    launchExecutor.shutdownNow()
    engines.values().asScala.foreach(me => closeQuietly(me.client))
    engines.clear()
  }

  // ------------------------------------------------------------- 内部

  /** 冷启动互斥：ZK 锁 + 锁内 double-check（他网关实例可能已拉起）。四 share level 统一
   * 走此路径；CONNECTION 的 lockPath 为用户级粒度（见类 javadoc）。 */
  private def launchUnderLatch(key: EngineKey): ManagedEngine = {
    zk.tryLock(key.lockPath, lockTimeoutMs, () => discoverOrLaunch(key))
  }

  private def discoverOrLaunch(key: EngineKey): ManagedEngine =
    discover(key.space).map(cache(key.space, _)).getOrElse(launch(key))

  private def discover(space: String): Option[EngineNode] = {
    val nodes = zk.listEngines(space).asScala
    if (nodes.isEmpty) None else Some(nodes.last) // 最新（sequence 最大）
  }

  private def launch(key: EngineKey): ManagedEngine = {
    val space = key.space
    val refId = java.util.UUID.randomUUID().toString
    val (connectPort, adminPort) =
      if (isClusterMode) {
        (config.getInt("fg.engine.spark.launch.cluster.connect-port"),
          config.getInt("fg.engine.spark.launch.cluster.admin-port"))
      } else {
        (PortRangeAllocator.allocate(config.getString("fg.engine.spark.launch.port-range")),
          PortRangeAllocator.allocate(config.getString("fg.engine.spark.launch.admin-port-range")))
      }

    if (!launchSlots.tryAcquire(initTimeoutMs, TimeUnit.MILLISECONDS)) {
      throw new IllegalStateException(
        s"Timeout acquiring engine launch permit ($maxConcurrentLaunches concurrent) for $space")
    }
    launchesCounter.increment()
    var launched: LaunchedEngine = null
    try {
      log.info(s"launching engine space=$space refId=$refId connectPort=$connectPort adminPort=$adminPort deploy=${config.getString("fg.engine.spark.launch.deploy-mode")}")
      launched = launcher.launch(key, space, refId, connectPort, adminPort)
      val deadline = System.currentTimeMillis() + initTimeoutMs
      var result: ManagedEngine = null
      while (result == null) {
        val found = zk.engineByRefId(space, refId) // java Optional
        if (found.isPresent) {
          result = cache(space, found.get())
          log.info(s"engine registered space=$space refId=$refId node=${result.node}")
        } else {
          if (launched.exitedAbnormally) {
            throw new IllegalStateException(
              s"engine process exited before registration (refId=$refId; see engine log)")
          }
          if (System.currentTimeMillis() > deadline) {
            launched.destroy()
            // cluster 模式此 destroy 只及 submitter（已早退）：卡死引擎无 admin 句柄可达，
            // 依赖引擎侧 idle 看门狗 / max-lifetime 自愈（D22-c 已知边界）
            throw new IllegalStateException(
              s"engine registration timeout after ${initTimeoutMs}ms (refId=$refId)")
          }
          Thread.sleep(1000)
        }
      }
      result
    } catch {
      case e: Throwable =>
        launchFailuresCounter.increment()
        throw e
    } finally {
      launchSlots.release()
    }
  }

  /** 缓存 space → client（并发下 putIfAbsent 收敛，败者用胜者实例）。 */
  private def cache(space: String, node: EngineNode): ManagedEngine = {
    val existing = engines.get(space)
    if (existing != null) return existing
    val adminPort = if (node.adminPort() > 0) node.adminPort() else config.getInt("fg.engine.spark.admin.port")
    val me = new ManagedEngine(
      new SparkEngineClient(config, s"sc://${node.host()}:${node.connectPort()}", adminPort),
      node, space)
    val winner = engines.putIfAbsent(space, me)
    if (winner != null) {
      closeQuietly(me.client)
      winner
    } else {
      me
    }
  }

  /** per-space 进程内门闩：同 space 并发冷启动/恢复收敛为一次执行。 */
  private def followLaunch(space: String, task: => ManagedEngine): ManagedEngine = {
    val future = launching.computeIfAbsent(space, _ => {
      val cf = new CompletableFuture[ManagedEngine]()
      launchExecutor.submit(() => {
        try cf.complete(task)
        catch {
          case t: Throwable => cf.completeExceptionally(t)
        } finally launching.remove(space, cf)
      })
      cf
    })
    try future.join()
    catch {
      case e: java.util.concurrent.CompletionException =>
        throw Option(e.getCause).getOrElse(e)
    }
  }

  /** housekeeping：缓存清点——znode 消失（引擎死/会话过期/自身重启换节点）即驱逐。 */
  private def evictDeadEngines(): Unit = {
    engines.values().asScala.foreach { me =>
      try {
        val stillThere = zk.listEngines(me.space).asScala
          .exists(n => me.node.znodeName() != null && me.node.znodeName() == n.znodeName())
        if (!stillThere) {
          engines.remove(me.space, me)
          closeQuietly(me.client)
          evictionsCounter("sweep").increment()
          log.debug(s"evicted engine ${me.space} (znode gone): ${me.node}")
        }
      } catch {
        case _: Exception => // ZK 抖动：本轮跳过
      }
    }
  }

  private def closeQuietly(client: SparkEngineClient): Unit = {
    try client.close()
    catch { case _: Exception => }
  }
}

/** 被管引擎实例：client（Connect channel + admin HTTP）+ 注册节点事实。 */
private[client] final class ManagedEngine(val client: SparkEngineClient, val node: EngineNode,
    val space: String)
