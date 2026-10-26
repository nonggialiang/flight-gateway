package org.fg.spark.client

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.{ClientCalls, StreamObserver}
import io.grpc.{CallOptions, ClientCall, ClientInterceptor, ManagedChannel, Metadata, MethodDescriptor}

import java.util.concurrent.TimeUnit

/**
 * Connect 通道（薄 stub）：官方生成 stub + 元数据拦截器。
 *
 * <p>提交即 detach 的取消语义经 {@link #startServerStreaming} 实现——手工持有
 * {@link ClientCall}，其 {@code cancel} 为 public 且必定生效。
 *
 * <p><b>D39 后补（实证修复）</b>：原 {@code CancelCapture} 拦截器在 {@code start()} 时
 * 捕获 {@code ClientCallStreamObserver} 的设想是 no-op——grpc 在拦截器链的 {@code start()}
 * 时刻，delegate 是 {@code ClientCallImpl}（实现 {@code ClientCall}，并非
 * {@code ClientCallStreamObserver}；后者要到 stub 层 {@code ClientCalls} 内部才被创建，
 * 拦截器不可见），断点实证 {@code observer=null}，"提交即 detach"从未生效——submit/
 * submitInline 的 executePlan 流一直是引擎侧活跃消费者（inline 的 reattach 消费者重放
 * schema 后饿死：活消息流向未取消的 submit 流，客户端 120s 超时 0 行）。
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

  /** 无取消需求的服务端流（reattach/attach）；元数据经拦截器注入。 */
  def serverStreamingStub[StubT](
      mk: io.grpc.Channel => StubT,
      user: String,
      sessionId: String): StubT =
    mk(
      io.grpc.ClientInterceptors.intercept(
        channel, java.util.Arrays.asList(attachHeadersInterceptor(user, sessionId))))

  /**
   * 可取消的服务端流（提交即 detach 用）：手工 {@code newCall} 后交 {@link ClientCalls}
   * 保底自动流量控制（与生成 stub 行为一致），返回 {@link ClientCall} 供调用方
   * {@code cancel(message, cause)}——必定生效，区别于原拦截器捕获式的静默 no-op。
   */
  def startServerStreaming[ReqT, RespT](
      method: MethodDescriptor[ReqT, RespT],
      user: String,
      sessionId: String,
      request: ReqT,
      responseObserver: StreamObserver[RespT]): ClientCall[ReqT, RespT] = {
    val intercepted = io.grpc.ClientInterceptors.intercept(
      channel, java.util.Arrays.asList(attachHeadersInterceptor(user, sessionId)))
    val call = intercepted.newCall(method, CallOptions.DEFAULT)
    ClientCalls.asyncServerStreamingCall(call, request, responseObserver)
    call
  }

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
