package org.fg.spark.client

import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import org.apache.spark.connect.proto.{ExecutePlanRequest, ExecutePlanResponse, SparkConnectServiceGrpc}
import org.fg.common.config.GatewayConfig
import org.fg.spi.{EngineExecutionHandle, GatewaySession, MaterializationSpec}
import org.junit.jupiter.api.{AfterEach, Assertions, BeforeEach, Test}

import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * D39 后补回归：提交即 detach 必须真实生效——submit/submitInline 首响应（operationId 回显）
 * 后，引擎侧应观测到客户端 cancel。原 CancelCapture 拦截器捕获式 cancel 是 no-op（grpc 在
 * 拦截器 start() 时 delegate 为 ClientCallImpl，非 ClientCallStreamObserver——断点实证
 * observer=null），inline 的 reattach 消费者因此只拿到缓冲重放（schema）后饿死：活消息全部
 * 流向未取消的 submit 流，客户端 ~120s 超时 0 行。
 *
 * <p>假引擎：executePlan 发一条带 operationId 的响应后挂住不 complete——客户端 detach（cancel）
 * 由 onCancelHandler 观测；修复前 cancel 永不到达（用例超时失败），修复后毫秒级到达。
 */
class SubmitDetachTest {

  private var server: Server = _
  private var engine: SparkEngineClient = _
  private var clientCancelled: CountDownLatch = _

  @BeforeEach
  def setUp(): Unit = {
    clientCancelled = new CountDownLatch(1)
    val service = new SparkConnectServiceGrpc.SparkConnectServiceImplBase {
      override def executePlan(
          request: ExecutePlanRequest,
          observer: StreamObserver[ExecutePlanResponse]): Unit = {
        val so = observer.asInstanceOf[io.grpc.stub.ServerCallStreamObserver[ExecutePlanResponse]]
        so.setOnCancelHandler(() => clientCancelled.countDown())
        // 首响应：回显 operationId（submit/submitInline 的捕获点）
        observer.onNext(
          ExecutePlanResponse.newBuilder()
            .setResponseId("r-1")
            .setOperationId(request.getOperationId)
            .build())
        // 刻意不继续发送也不 complete：流必须由客户端 detach（cancel）收口
      }
    }
    server = NettyServerBuilder.forPort(0).addService(service).build().start()
    engine = new SparkEngineClient(
      GatewayConfig.create(), s"sc://localhost:${server.getPort}", 0)
  }

  @AfterEach
  def tearDown(): Unit = {
    engine.close()
    server.shutdownNow()
    server.awaitTermination(5, TimeUnit.SECONDS)
  }

  private def session: org.fg.spi.EngineSession =
    engine.openSession(new GatewaySession("s-1", "alice"))

  /** 物化提交：首响应捕获 opId → future 完成 → 引擎侧观测 cancel。 */
  @Test
  def submitDetachesAfterOperationIdEcho(): Unit = {
    val handle = new EngineExecutionHandle("spark", UUID.randomUUID().toString)
    val onHandle = new CountDownLatch(1)
    val future = engine.submit(
      session, "SELECT 1",
      new MaterializationSpec("s3a://bucket/results/spark/alice/q/", "zstd", 16384, null),
      handle,
      (_: org.fg.spi.EngineExecutionHandle) => onHandle.countDown())

    val echoed = future.get(5, TimeUnit.SECONDS)
    Assertions.assertEquals(handle.handle(), echoed.handle())
    Assertions.assertTrue(onHandle.await(5, TimeUnit.SECONDS), "listener.onHandle 应随首响应触发")
    Assertions.assertTrue(
      clientCancelled.await(5, TimeUnit.SECONDS),
      "提交即 detach：服务端应观测到客户端 cancel（原 CancelCapture no-op 回归）")
  }

  /** inline 提交：同款 detach 语义（reattach 消费者依赖此成立）。 */
  @Test
  def submitInlineDetachesAfterOperationIdEcho(): Unit = {
    val handle = new EngineExecutionHandle("spark", UUID.randomUUID().toString)
    val onHandle = new CountDownLatch(1)
    val future = engine.submitInline(
      session, "SELECT 1", handle,
      (_: org.fg.spi.EngineExecutionHandle) => onHandle.countDown())

    val echoed = future.get(5, TimeUnit.SECONDS)
    Assertions.assertEquals(handle.handle(), echoed.handle())
    Assertions.assertTrue(onHandle.await(5, TimeUnit.SECONDS), "listener.onHandle 应随首响应触发")
    Assertions.assertTrue(
      clientCancelled.await(5, TimeUnit.SECONDS),
      "提交即 detach：服务端应观测到客户端 cancel（原 CancelCapture no-op 回归）")
  }
}
