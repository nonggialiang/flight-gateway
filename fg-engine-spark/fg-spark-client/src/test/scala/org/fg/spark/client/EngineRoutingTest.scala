package org.fg.spark.client

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.fg.common.config.GatewayConfig
import org.fg.spi.{EngineExecutionHandle, GatewaySession}
import org.junit.jupiter.api.{AfterEach, Assertions, BeforeEach, Test}

import java.util.UUID

/**
 * share level → engine space 路由表（D22 决策 2）：SERVER=常量路由用户、GROUP=组映射、
 * USER/CONNECTION=认证用户；CONNECTION 追加 refId=fg 会话 id。附：group 映射、端口段、
 * opId→space 绑定的 release 语义（决策 2 例外分支）。
 */
class EngineRoutingTest {

  private val props = Seq(
    "fg.zk.namespace", "fg.engine.share.level", "fg.engine.share.subdomain",
    "fg.engine.share.server-user", "fg.engine.share.group-mapping")

  @BeforeEach
  @AfterEach
  def resetProps(): Unit = props.foreach(System.clearProperty)

  private def config(): GatewayConfig = GatewayConfig.create()

  private def router(): SparkEngineRouter =
    new SparkEngineRouter(config(), null, null, new SimpleMeterRegistry())

  private def session(user: String, sid: String): SparkEngineSession =
    new SparkEngineSession(new GatewaySession(sid, user))

  private def keySpace(user: String, sid: String): String =
    router().keyOf(session(user, sid)).space

  @Test
  def userShareRoutesPerUser(): Unit = {
    System.setProperty("fg.zk.namespace", "ns-x")
    System.setProperty("fg.engine.share.level", "USER")
    Assertions.assertEquals("/ns-x_v1_USER_spark/fg/default", keySpace("fg", UUID.randomUUID().toString))
    Assertions.assertEquals("/ns-x_v1_USER_spark/fg2/default", keySpace("fg2", UUID.randomUUID().toString))
    // 同用户不同会话 → 同 space（复用）
    Assertions.assertEquals(keySpace("fg", UUID.randomUUID().toString), keySpace("fg", UUID.randomUUID().toString))
  }

  @Test
  def serverShareRoutesConstantUser(): Unit = {
    System.setProperty("fg.zk.namespace", "ns-x")
    System.setProperty("fg.engine.share.level", "SERVER")
    System.setProperty("fg.engine.share.server-user", "shared-engine")
    Assertions.assertEquals("/ns-x_v1_SERVER_spark/shared-engine/default",
      keySpace("fg", UUID.randomUUID().toString))
    Assertions.assertEquals(keySpace("fg", UUID.randomUUID().toString),
      keySpace("fg2", UUID.randomUUID().toString))
  }

  @Test
  def groupShareRoutesPrimaryGroup(): Unit = {
    System.setProperty("fg.zk.namespace", "ns-x")
    System.setProperty("fg.engine.share.level", "GROUP")
    System.setProperty("fg.engine.share.group-mapping", "fg:teamA,fg2:teamA,fg3:teamB")
    Assertions.assertEquals("/ns-x_v1_GROUP_spark/teamA/default", keySpace("fg", UUID.randomUUID().toString))
    Assertions.assertEquals("/ns-x_v1_GROUP_spark/teamA/default", keySpace("fg2", UUID.randomUUID().toString))
    Assertions.assertEquals("/ns-x_v1_GROUP_spark/teamB/default", keySpace("fg3", UUID.randomUUID().toString))
    // 未映射用户回落 user 本身（文档化）
    Assertions.assertEquals("/ns-x_v1_GROUP_spark/other/default", keySpace("other", UUID.randomUUID().toString))
  }

  @Test
  def connectionShareEmbedsSessionRefId(): Unit = {
    System.setProperty("fg.zk.namespace", "ns-x")
    System.setProperty("fg.engine.share.level", "CONNECTION")
    val sidA = UUID.randomUUID().toString
    val sidB = UUID.randomUUID().toString
    Assertions.assertEquals(s"/ns-x_v1_CONNECTION_spark/fg/default/$sidA", keySpace("fg", sidA))
    Assertions.assertNotEquals(keySpace("fg", sidA), keySpace("fg", sidB))
  }

  @Test
  def subdomainSegmentHonored(): Unit = {
    System.setProperty("fg.zk.namespace", "ns-x")
    System.setProperty("fg.engine.share.level", "USER")
    System.setProperty("fg.engine.share.subdomain", "etl")
    Assertions.assertEquals("/ns-x_v1_USER_spark/fg/etl", keySpace("fg", UUID.randomUUID().toString))
  }

  @Test
  def lockPathIsSiblingOfRoot(): Unit = {
    System.setProperty("fg.zk.namespace", "ns-x")
    System.setProperty("fg.engine.share.level", "USER")
    val key = router().keyOf(session("fg", UUID.randomUUID().toString))
    Assertions.assertEquals("/ns-x_v1_USER_spark_lock/fg/default", key.lockPath)
    Assertions.assertFalse(key.isConnection)
  }

  @Test
  def staticGroupProviderParsesEntries(): Unit = {
    System.setProperty("fg.engine.share.group-mapping", "a:g1,b:g2")
    val p = new StaticGroupProvider(config())
    Assertions.assertEquals("g1", p.primaryGroup("a"))
    Assertions.assertEquals("g2", p.primaryGroup("b"))
    Assertions.assertEquals("c", p.primaryGroup("c")) // 未映射回落
  }

  @Test
  def portRangeAllocatorParsesAndAllocates(): Unit = {
    Assertions.assertEquals((16000, 16999), PortRangeAllocator.parseRange("16000-16999"))
    Assertions.assertEquals((15002, 15002), PortRangeAllocator.parseRange("15002"))
    val a = PortRangeAllocator.allocate("16000-16999")
    Assertions.assertTrue(a >= 16000 && a <= 16999)
    val b = PortRangeAllocator.allocate("16000-16999")
    Assertions.assertTrue(b >= 16000 && b <= 16999)
  }

  @Test
  def operationBindingsConsumeIsOneShot(): Unit = {
    val bindings = new OperationBindings(100)
    bindings.record("op1", "/s1")
    bindings.record("op2", "/s2")
    Assertions.assertEquals(Some("/s1"), bindings.consume("op1"))
    Assertions.assertEquals(None, bindings.consume("op1")) // 一次性
    Assertions.assertEquals(Some("/s2"), bindings.consume("op2"))
    Assertions.assertEquals(None, bindings.consume("unknown"))
  }

  // ---- releaseExecution 语义（定向命中 / miss 广播；经 manager 桩观察决策，不发 RPC） ----

  /** 测试桩：覆写 clientBySpace/allClients 记录 release 定向决策。 */
  private class RecordingManager(config: GatewayConfig)
      extends SparkEngineManager(config, null, new SimpleMeterRegistry()) {
    var bySpaceQueries: Seq[String] = Nil
    var broadcastCount = 0

    override def clientBySpace(space: String): Option[SparkEngineClient] = {
      bySpaceQueries :+= space; None
    }

    override def allClients: Seq[SparkEngineClient] = {
      broadcastCount += 1; Nil
    }
  }

  private def swapManager[T](router: SparkEngineRouter, manager: SparkEngineManager)(body: => T): T = {
    val field = classOf[SparkEngineRouter].getDeclaredField("manager")
    field.setAccessible(true)
    val original = field.get(router)
    field.set(router, manager)
    try body
    finally field.set(router, original)
  }

  private def recordBinding(router: SparkEngineRouter, opId: String, space: String): Unit = {
    val field = classOf[SparkEngineRouter].getDeclaredField("opBindings")
    field.setAccessible(true)
    field.get(router).asInstanceOf[OperationBindings].record(opId, space)
  }

  @Test
  def releaseExecutionUnknownOpBroadcasts(): Unit = {
    val r = router()
    val recording = new RecordingManager(config())
    swapManager(r, recording) {
      r.releaseExecution(new EngineExecutionHandle("spark", "never-seen"))
    }
    Assertions.assertEquals(1, recording.broadcastCount)
    Assertions.assertTrue(recording.bySpaceQueries.isEmpty)
  }

  @Test
  def releaseExecutionBoundOpTargetsSpaceThenFallsBack(): Unit = {
    val r = router()
    val recording = new RecordingManager(config())
    swapManager(r, recording) {
      recordBinding(r, "op-9", "/space-nine")
      r.releaseExecution(new EngineExecutionHandle("spark", "op-9"))
      Assertions.assertEquals(0, recording.broadcastCount)
      Assertions.assertEquals(Seq("/space-nine"), recording.bySpaceQueries)
      // 消费后同 op 再 release → 广播回落
      r.releaseExecution(new EngineExecutionHandle("spark", "op-9"))
      Assertions.assertEquals(1, recording.broadcastCount)
    }
  }

  @Test
  def recoveryConfinedToSessionStatusByContract(): Unit = {
    // 决策 5 的结构断言：router 中 recover 只被 sessionStatus 调用（源码级约束，防回归）
    val raw = scala.io.Source.fromFile(
      "src/main/scala/org/fg/spark/client/SparkEngineRouter.scala").mkString
    val code = raw.replaceAll("(?s)/\\*.*?\\*/", "").replaceAll("//[^\n]*", "") // 去注释
    val occurrences = "manager.recover".r.findAllIn(code).length
    val inSessionStatus = code.split("sessionStatus").exists(_.contains("manager.recover"))
    Assertions.assertEquals(1, occurrences, "recover 调用点必须唯一（sessionStatus）")
    Assertions.assertTrue(inSessionStatus, "recover 只允许出现在 sessionStatus 内")
  }
}
