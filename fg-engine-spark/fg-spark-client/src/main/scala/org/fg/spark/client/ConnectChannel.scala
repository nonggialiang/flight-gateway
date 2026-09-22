package org.fg.spark.client

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.{ClientInterceptor, ForwardingClientCall}
import io.grpc.stub.ClientCallStreamObserver
import io.grpc.{ClientCall, CallOptions, ManagedChannel, Metadata, MethodDescriptor}

import java.util.concurrent.TimeUnit

/**
 * Connect 通道（薄 stub）：官方生成 stub + 元数据拦截器。
 *
 * <p>detach 语义经 {@link CancelCapture} 拦截器捕获 ClientCallStreamObserver 实现
 * （提交即 detach：首响应捕获 operationId 后 cancel 流）。
 */
final class ConnectChannel(uri: String) extends AutoCloseable {

  private val (host, port) = ConnectChannel.parse(uri)
  val channel: ManagedChannel =
    NettyChannelBuilder.forAddress(host, port).usePlaintext().build()

  private def headers(user: String, sessionId: String): Metadata = {
    val m = new Metadata()
    m.put(Metadata.Key.of("user_id", Metadata.ASCII_STRING_MARSHALLER), user)
    m.put(Metadata.Key.of("session_id", Metadata.ASCII_STRING_MARSHALLER), sessionId)
    m
  }

  /** 附加 user_id/session_id 元数据的拦截器。 */
  def attachHeadersInterceptor(user: String, sessionId: String): ClientInterceptor =
    io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers(user, sessionId))

  /** 无超时的服务端流（execute/reattach）；cancel 捕获器由调用方持有。 */
  def serverStreamingStub[StubT](
      mk: io.grpc.Channel => StubT,
      user: String,
      sessionId: String,
      cancelCapture: CancelCapture): StubT =
    mk(
      io.grpc.ClientInterceptors.intercept(
        channel,
        java.util.Arrays.asList(attachHeadersInterceptor(user, sessionId), cancelCapture)))

  /** 带超时的 unary stub（AnalyzePlan/Interrupt/ReleaseExecute）。 */
  def unaryStub[StubT](mk: io.grpc.Channel => StubT, user: String, sessionId: String,
      timeoutMs: Long): StubT = {
    val intercepted = io.grpc.ClientInterceptors.intercept(
      channel, java.util.Arrays.asList(attachHeadersInterceptor(user, sessionId)))
    mk(intercepted)
  }

  override def close(): Unit = {
    channel.shutdownNow()
    channel.awaitTermination(5, TimeUnit.SECONDS)
  }
}

/** 捕获底层 ClientCallStreamObserver 以支持 cancel（提交即 detach）。 */
final class CancelCapture extends ClientInterceptor {
  @volatile private var observer: ClientCallStreamObserver[_] = _

  def cancel(message: String): Unit = {
    val obs = observer
    if (obs != null) obs.cancel(message, null)
  }

  override def interceptCall[ReqT, RespT](
      method: MethodDescriptor[ReqT, RespT],
      callOptions: CallOptions,
      next: io.grpc.Channel): ClientCall[ReqT, RespT] =
    new ForwardingClientCall.SimpleForwardingClientCall[ReqT, RespT](next.newCall(method, callOptions)) {
      override def start(
          responseListener: ClientCall.Listener[RespT],
          headers: Metadata): Unit = {
        delegate match {
          case obs: ClientCallStreamObserver[RespT] => observer = obs
          case _ =>
        }
        super.start(responseListener, headers)
      }
    }
}

private[client] object ConnectChannel {
  def parse(uri: String): (String, Int) = {
    val stripped = uri.replaceFirst("^sc://", "").replaceFirst("^spark://", "")
    stripped.split(":") match {
      case Array(h, p) => (h, p.toInt)
      case Array(h) => (h, 15002)
      case _ => throw new IllegalArgumentException(s"Malformed connect uri: $uri")
    }
  }
}
