package org.fg.frontend;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.flight.auth2.Auth2Constants;

/**
 * per-connection 会话兜底通道（M3 "cookie 双轨" 提前）：捕获 Authorization 头原值
 * （Basic/Bearer），供 producer 在客户端未走 cookie 会话时派生<b>按连接</b>的
 * sessionRef——Bearer token 每连接一次握手、连接内稳定；Basic 直发的退化场景同值稳定。
 *
 * <p>捕获值不直接落库：sessionRef 只取其 sha256 前缀（凭证材料不进 fg_operation/日志），
 * 见 {@code FgFlightProducer#sessionRef}。
 */
final class AuthHeaderMiddleware implements FlightServerMiddleware {

  /** middleware 注册键。 */
  static final FlightServerMiddleware.Key<AuthHeaderMiddleware> KEY =
      FlightServerMiddleware.Key.of("fg-auth-header");

  private final String authorization;

  private AuthHeaderMiddleware(String authorization) {
    this.authorization = authorization;
  }

  /** 本调用的 Authorization 头原值（null=未携带，auth2-only 下理论不可达）。 */
  String authorization() {
    return authorization;
  }

  /** 每 RPC 一次：从传入 headers 捕获 Authorization。 */
  static final class Factory implements FlightServerMiddleware.Factory<AuthHeaderMiddleware> {

    @Override
    public AuthHeaderMiddleware onCallStarted(
        CallInfo info, CallHeaders incomingHeaders, RequestContext context) {
      return new AuthHeaderMiddleware(incomingHeaders.get(Auth2Constants.AUTHORIZATION_HEADER));
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
