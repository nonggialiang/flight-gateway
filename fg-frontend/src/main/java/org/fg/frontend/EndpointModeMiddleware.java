package org.fg.frontend;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

/**
 * D15 endpoint 模式协商（header 通道）：每个 RPC 经 {@link Factory#onCallStarted} 捕获请求头
 * {@link #HEADER}（值 {@code https|relay}，大小写不敏感），producer 在注册时经
 * {@code CallContext.getMiddleware(KEY)} 读取并落行——mode 一旦落行，后续同指纹 RPC 均按行构造，
 * 不再受头变化影响（"注册时协商落行"语义）。
 *
 * <p>未携带或非法值由 producer 侧回退服务端配置 {@code fg.result.endpoint.mode}（默认 relay），
 * 中间件只做透传不做裁决。session option 通道（Flight SQL SetSessionOptions）仍归 M3。
 */
final class EndpointModeMiddleware implements FlightServerMiddleware {

  /** 请求头名：客户端声明本次注册期望的取数形态（HTTPS presign / RELAY 中继）。 */
  static final String HEADER = "x-fg-endpoint-mode";

  /** middleware 注册键（引用等价，实例共享）。 */
  static final FlightServerMiddleware.Key<EndpointModeMiddleware> KEY =
      FlightServerMiddleware.Key.of("fg-endpoint-mode");

  private final String requestedMode;

  private EndpointModeMiddleware(String requestedMode) {
    this.requestedMode = requestedMode;
  }

  /** 本调用携带的模式头值（null=未携带）；合法性与回退由 producer 的 negotiateMode 收口。 */
  String requestedMode() {
    return requestedMode;
  }

  /** 每 RPC 一次：从传入 headers 捕获模式声明。 */
  static final class Factory implements FlightServerMiddleware.Factory<EndpointModeMiddleware> {

    @Override
    public EndpointModeMiddleware onCallStarted(
        CallInfo info, CallHeaders incomingHeaders, RequestContext context) {
      return new EndpointModeMiddleware(incomingHeaders.get(HEADER));
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
