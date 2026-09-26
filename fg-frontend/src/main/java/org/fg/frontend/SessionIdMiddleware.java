package org.fg.frontend;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

/**
 * D20 会话身份头通道：客户端自报会话 id（{@code x-fg-session-id}，UUID）——无 cookie 能力
 * 的客户端（pyarrow 手工线经 FlightCallOptions/ADBC 经
 * {@code adbc.flight.sql.rpc.call_header.x-fg-session-id}）由此获得 per-connection 会话身份。
 * 收到 session closed 错误后客户端自行轮换 id 重建会话（服务端不复活、不翻新）。
 *
 * <p>纯捕获型 middleware：合法性（非空/长度）与 cookie 通道的优先级在 producer 的
 * sessionRef 收口。
 */
final class SessionIdMiddleware implements FlightServerMiddleware {

  /** 请求头名：客户端自报的 fg 会话身份。 */
  static final String HEADER = "x-fg-session-id";

  /** middleware 注册键（引用等价，实例共享）。 */
  static final FlightServerMiddleware.Key<SessionIdMiddleware> KEY =
      FlightServerMiddleware.Key.of("fg-session-id");

  private final String sessionId;

  private SessionIdMiddleware(String sessionId) {
    this.sessionId = sessionId;
  }

  /** 本调用携带的自报会话 id（null=未携带）。 */
  String sessionId() {
    return sessionId;
  }

  /** 每 RPC 一次：从传入 headers 捕获自报会话 id。 */
  static final class Factory implements FlightServerMiddleware.Factory<SessionIdMiddleware> {

    @Override
    public SessionIdMiddleware onCallStarted(
        CallInfo info, CallHeaders incomingHeaders, RequestContext context) {
      return new SessionIdMiddleware(incomingHeaders.get(HEADER));
    }
  }

  // 纯捕获型 middleware：发送头/完成/异常钩子均无动作

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {}

  @Override
  public void onCallCompleted(CallStatus status) {}

  @Override
  public void onCallErrored(Throwable err) {}
}
