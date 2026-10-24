package org.fg.spark.client

import org.fg.spi.ExecutionOutcome.OutcomeStatus
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

/** attach onError 终态分流（D35）：传输类→UNKNOWN（manifest 对账），执行类→FAILED 快败。 */
class AttachErrorClassificationTest {

  private def grpcError(code: io.grpc.Status.Code, desc: String): Throwable =
    new io.grpc.StatusRuntimeException(io.grpc.Status.fromCode(code).withDescription(desc))

  @Test
  def cancelledMapsToCancelled(): Unit = {
    val o = SparkEngineClient.classifyAttachError(grpcError(io.grpc.Status.Code.CANCELLED, ""))
    assertEquals(OutcomeStatus.CANCELLED, o.status())
  }

  @Test
  def transportErrorsMapToUnknown(): Unit = {
    // 引擎不可达（连接断/reset）——执行状态不可判定，交 manifest 对账
    assertEquals(
      OutcomeStatus.UNKNOWN,
      SparkEngineClient
        .classifyAttachError(grpcError(io.grpc.Status.Code.UNAVAILABLE, "connection reset"))
        .status())
    assertEquals(
      OutcomeStatus.UNKNOWN,
      SparkEngineClient
        .classifyAttachError(grpcError(io.grpc.Status.Code.DEADLINE_EXCEEDED, "deadline"))
        .status())
  }

  @Test
  def executionErrorsMapToFailedWithRealMessage(): Unit = {
    // Spark 运行时异常经 GrpcExceptionConverter 投递（INTERNAL/UNKNOWN 等）——真实异常直达客户端
    val desc = "org.apache.spark.SparkException: Job aborted due to stage failure: Task OOM"
    val o = SparkEngineClient
      .classifyAttachError(grpcError(io.grpc.Status.Code.INTERNAL, desc))
    assertEquals(OutcomeStatus.FAILED, o.status())
    assertTrue(o.error().contains(desc), s"error should carry the engine message: ${o.error()}")
  }

  @Test
  def unknownGrpcCodeMapsToFailed(): Unit = {
    // GrpcExceptionConverter 对多数 Spark 异常用 UNKNOWN 状态
    val o = SparkEngineClient
      .classifyAttachError(grpcError(io.grpc.Status.Code.UNKNOWN, "AnalysisException: ..."))
    assertEquals(OutcomeStatus.FAILED, o.status())
  }

  @Test
  def notFoundMapsToUnknownWithOperationNotFoundPrefix(): Unit = {
    // D36：reattach 寻址不到 operationId（submit 未达引擎/引擎重启清空）→ unknown（前缀
    // "operation not found"），orchestrator 侧按 orphan-grace 宽限后判死
    val o = SparkEngineClient
      .classifyAttachError(grpcError(io.grpc.Status.Code.NOT_FOUND, "no such operation"))
    assertEquals(OutcomeStatus.UNKNOWN, o.status())
    assertTrue(o.error().startsWith("operation not found"), o.error())

    val o2 = SparkEngineClient
      .classifyAttachError(grpcError(io.grpc.Status.Code.INVALID_ARGUMENT, "unknown operation id"))
    assertEquals(OutcomeStatus.UNKNOWN, o2.status())
    assertTrue(o2.error().startsWith("operation not found"), o2.error())
  }

  @Test
  def nonGrpcThrowableMapsToUnknown(): Unit = {
    assertEquals(
      OutcomeStatus.UNKNOWN,
      SparkEngineClient.classifyAttachError(new RuntimeException("local")).status())
  }
}
