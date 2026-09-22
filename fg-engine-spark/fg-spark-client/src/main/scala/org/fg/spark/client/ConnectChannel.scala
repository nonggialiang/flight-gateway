package org.fg.spark.client

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.{CallOptions, ClientCall, ManagedChannel, Metadata, MethodDescriptor, Status}

import java.util.concurrent.{CompletableFuture, TimeUnit}

/** Connect 通道（薄 stub）：管理 channel、元数据头、可取消的服务端流与 unary 调用。 */
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

  /**
   * 服务端流调用，返回可取消句柄。onMessage 每响应一次；流终经 onDone(status)。
   * 用于 execute（提交即 detach：首响应后 cancel）与 reattach（消费到流终）。
   */
  private[client] def serverStream[Req, Resp](
      method: MethodDescriptor[Req, Resp],
      request: Req,
      user: String,
      sessionId: String,
      onMessage: Resp => Unit,
      onDone: Status => Unit): () => Unit = {
    val call: ClientCall[Req, Resp] =
      channel.newCall(method, CallOptions.DEFAULT)
    startStream(call, request, user, sessionId, onMessage, onDone)
  }

  /** unary 调用（AnalyzePlan/Interrupt/ReleaseExecute）：带超时。 */
  private[client] def unary[Req, Resp](
      method: MethodDescriptor[Req, Resp],
      request: Req,
      user: String,
      sessionId: String,
      timeoutMs: Long): CompletableFuture[Resp] = {
    val future = new CompletableFuture[Resp]()
    val call: ClientCall[Req, Resp] =
      channel.newCall(method, CallOptions.DEFAULT.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS))
    startStream[Req, Resp](
      call,
      request,
      user,
      sessionId,
      onMessage = (resp: Resp) => future.complete(resp),
      onDone = (status: Status) =>
        if (!future.isDone) {
          future.completeExceptionally(
            if (status.getCause != null) status.getCause
            else new IllegalStateException(s"unary call failed: $status"))
        }
    )
    future
  }

  private def startStream[Req, Resp](
      call: ClientCall[Req, Resp],
      request: Req,
      user: String,
      sessionId: String,
      onMessage: Resp => Unit,
      onDone: Status => Unit): () => Unit = {
    val listener = new io.grpc.ClientCall.Listener[Resp] {
      private var done = false
      override def onMessage(message: Resp): Unit = synchronized {
        if (!done) onMessage(message)
      }
      override def onClose(status: Status, trailers: Metadata): Unit = synchronized {
        if (!done) {
          done = true
          onDone(status)
        }
      }
    }
    call.start(listener, headers(user, sessionId))
    call.request(2)
    call.sendMessage(request)
    call.halfClose()
    () => call.cancel("fg client cancelled", null)
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
