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

  /** D39b 排障延伸：验证我们的 reattach 消费端能完整接收多消息流（排除 grpc 客户端
    * 只 request(1) 不续读的可能——那也会表现成"恰好一条消息后静默"）。假 reattach 连发
    * schema + 2×batch + result_complete，InlineConsumer 必须全部收到。 */
  @Test
  def attachInlineConsumesMultiMessageReplay(): Unit = {
    val schemaPayload = org.apache.spark.connect.proto.ExecutePlanResponse.newBuilder()
      .setResponseId("r-1")
      .setOperationId("op-multi")
      .setSchema(org.apache.spark.connect.proto.DataType.newBuilder()
        .setStruct(org.apache.spark.connect.proto.DataType.Struct.newBuilder()))
      .build()
    val service = new SparkConnectServiceGrpc.SparkConnectServiceImplBase {
      override def reattachExecute(
          request: org.apache.spark.connect.proto.ReattachExecuteRequest,
          observer: StreamObserver[ExecutePlanResponse]): Unit = {
        val batchProto = org.apache.spark.connect.proto.ExecutePlanResponse.newBuilder()
          .setResponseId("r-2")
          .setArrowBatch(
            org.apache.spark.connect.proto.ExecutePlanResponse.ArrowBatch.newBuilder()
              .setRowCount(1)
              .setData(com.google.protobuf.ByteString.copyFromUtf8("fake-ipc")))
          .build()
        // 连发（重放缓冲的真实形态——修复前实测丢第 2 条）
        observer.onNext(schemaPayload)
        observer.onNext(batchProto.toBuilder().setResponseId("r-2").build())
        observer.onNext(batchProto.toBuilder().setResponseId("r-3").build())
        observer.onNext(org.apache.spark.connect.proto.ExecutePlanResponse.newBuilder()
          .setResponseId("r-4")
          .setResultComplete(
            org.apache.spark.connect.proto.ExecutePlanResponse.ResultComplete.getDefaultInstance)
          .build())
        observer.onCompleted()
      }
    }
    val srv = NettyServerBuilder.forPort(0).addService(service).build().start()
    val eng = new SparkEngineClient(GatewayConfig.create(), s"sc://localhost:${srv.getPort}", 0)
    try {
      val schemas = new java.util.concurrent.atomic.AtomicInteger()
      val batches = java.util.Collections.synchronizedList(new java.util.ArrayList[String]())
      val completes = new java.util.concurrent.atomic.AtomicInteger()
      val outcome = eng
        .attachInline(
          eng.openSession(new GatewaySession("s-1", "alice")),
          new EngineExecutionHandle("spark", "op-multi"),
          null,
          new org.fg.spi.SqlEngine.InlineConsumer {
            override def onSchema(s: org.apache.arrow.vector.types.pojo.Schema): Unit =
              schemas.incrementAndGet()
            override def onBatch(responseId: String, ipcStreamBytes: Array[Byte]): Unit =
              batches.add(responseId)
            override def onResultComplete(): Unit = completes.incrementAndGet()
          })
        .get(5, TimeUnit.SECONDS)
      Assertions.assertEquals(1, schemas.get, "onSchema 应恰好一次")
      Assertions.assertEquals(java.util.Arrays.asList("r-2", "r-3"), batches, "两批应全量到达")
      Assertions.assertEquals(1, completes.get, "onResultComplete 应恰好一次")
      Assertions.assertEquals(org.fg.spi.ExecutionOutcome.OutcomeStatus.COMPLETED, outcome.status())
    } finally {
      eng.close()
      srv.shutdownNow()
      srv.awaitTermination(5, TimeUnit.SECONDS)
    }
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
