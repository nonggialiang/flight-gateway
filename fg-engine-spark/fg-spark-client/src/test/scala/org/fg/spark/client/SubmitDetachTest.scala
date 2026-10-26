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
 * D39b/D40 回归：
 * ① 物化 submit 的"提交即 detach"真实生效（CancelCapture no-op 事故回归）；
 * ② inline submitInline = 非 reattachable 挂流直消——请求不带 ReattachOptions、流保持
 *    打开（服务端不观测 cancel）、首响应（opId+schema）即 submitted 信号、消费端
 *    requestMore 逐条拉取（手动流控），流终 outcome=COMPLETED。
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
            .setSchema(org.apache.spark.connect.proto.DataType.newBuilder()
              .setStruct(org.apache.spark.connect.proto.DataType.Struct.newBuilder()))
            .build())
        // 刻意不继续发送也不 complete：物化 submit 须由客户端 detach（cancel）收口
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

  /** 物化提交（QUERY 路径）：首响应捕获 opId → future 完成 → 引擎侧观测 cancel。 */
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

  /**
   * D40 inline 挂流直消：请求无 ReattachOptions、首响应即 submitted、流保持打开（无
   * cancel）、requestMore 逐条拉取全部消息、流终 COMPLETED。
   */
  @Test
  def submitInlineHoldsStreamAndDeliversOnDemand(): Unit = {
    val capturedRequest = new java.util.concurrent.atomic.AtomicReference[ExecutePlanRequest]()
    val batchProto = ExecutePlanResponse.newBuilder()
      .setResponseId("r-2")
      .setArrowBatch(
        org.apache.spark.connect.proto.ExecutePlanResponse.ArrowBatch.newBuilder()
          .setRowCount(1)
          .setData(com.google.protobuf.ByteString.copyFromUtf8("fake-ipc")))
      .build()
    val service = new SparkConnectServiceGrpc.SparkConnectServiceImplBase {
      override def executePlan(
          request: ExecutePlanRequest,
          observer: StreamObserver[ExecutePlanResponse]): Unit = {
        capturedRequest.set(request)
        observer.onNext(
          ExecutePlanResponse.newBuilder()
            .setResponseId("r-1")
            .setOperationId(request.getOperationId)
            .setSchema(org.apache.spark.connect.proto.DataType.newBuilder()
              .setStruct(org.apache.spark.connect.proto.DataType.Struct.newBuilder()))
            .build())
        // 连发余量（客户端手动流控按 requestMore 逐条放行——已到消息在客户端入站队列排队）
        observer.onNext(batchProto.toBuilder().setResponseId("r-2").build())
        observer.onNext(batchProto.toBuilder().setResponseId("r-3").build())
        observer.onNext(ExecutePlanResponse.newBuilder()
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
      val handle = new EngineExecutionHandle("spark", "op-d40")
      val onHandle = new CountDownLatch(1)
      val stream = eng.submitInline(
        eng.openSession(new GatewaySession("s-1", "alice")),
        "SELECT 1",
        handle,
        (_: org.fg.spi.EngineExecutionHandle) => onHandle.countDown())

      // 首响应（submitted 信号）
      stream.submitted().get(5, TimeUnit.SECONDS)
      Assertions.assertTrue(onHandle.await(1, TimeUnit.SECONDS), "listener.onHandle 应随首响应触发")
      // 非 reattachable：请求不携带 ReattachOptions
      val req = capturedRequest.get
      Assertions.assertEquals(
          0,
          req.getRequestOptionsCount,
          "inline executePlan 不应携带 requestOptions（reattachable 已弃用）")

      val schemas = new java.util.concurrent.atomic.AtomicInteger()
      val batches = java.util.Collections.synchronizedList(new java.util.ArrayList[String]())
      val completes = new java.util.concurrent.atomic.AtomicInteger()
      stream.attach(new org.fg.spi.SqlEngine.InlineConsumer {
        override def onSchema(s: org.apache.arrow.vector.types.pojo.Schema): Unit =
          schemas.incrementAndGet()
        override def onBatch(responseId: String, ipcStreamBytes: Array[Byte]): Unit =
          batches.add(responseId)
        override def onResultComplete(): Unit = completes.incrementAndGet()
      })

      // 逐条拉取：每条交付后才 requestMore
      stream.requestMore()
      stream.requestMore()
      stream.requestMore()

      val outcome = stream.outcome().get(5, TimeUnit.SECONDS)
      Assertions.assertEquals(1, schemas.get, "onSchema 应恰好一次（首响应投递）")
      Assertions.assertEquals(java.util.Arrays.asList("r-2", "r-3"), batches, "两批应全量到达")
      Assertions.assertEquals(1, completes.get, "onResultComplete 应恰好一次")
      Assertions.assertEquals(org.fg.spi.ExecutionOutcome.OutcomeStatus.COMPLETED, outcome.status())
    } finally {
      eng.close()
      srv.shutdownNow()
      srv.awaitTermination(5, TimeUnit.SECONDS)
    }
  }
}
