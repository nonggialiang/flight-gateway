package org.fg.frontend;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

/**
 * D27 scroll 随机翻页判据（header 通道，EndpointModeMiddleware 同范式）：每个 RPC 经
 * {@link Factory#onCallStarted} 捕获 {@link #HEADER}（FG JDBC fork 对
 * {@code createStatement(TYPE_SCROLL_INSENSITIVE, ...)} 的 statement 自动携带
 * {@code scroll}），producer 注册时读取——scroll 即落行 {@code fg_operation.scrollable=true}
 * 且 mode 强制 RELAY（优先级链：scroll > x-fg-endpoint-mode > fg.result.endpoint.mode）。
 *
 * <p>与 D15 mode 同款"注册时协商落行"语义：落行后同指纹 RPC 按行构造，不再受头变化影响。
 * 非 fork 客户端不携带此头 = 完全走 FORWARD_ONLY 既有行为（回归门）。
 */
final class ScrollModeMiddleware implements FlightServerMiddleware {

  /** 请求头名：客户端声明本 statement 为 TYPE_SCROLL_INSENSITIVE（服务端切片随机翻页）。 */
  static final String HEADER = "x-fg-result-set-type";

  /** 唯一合法值（TYPE_SCROLL_SENSITIVE 由 fork 侧降级为 scroll 后再携带）。 */
  static final String SCROLL_VALUE = "scroll";

  /** middleware 注册键（引用等价，实例共享）。 */
  static final FlightServerMiddleware.Key<ScrollModeMiddleware> KEY =
      FlightServerMiddleware.Key.of("fg-result-set-type");

  private final boolean scroll;

  private ScrollModeMiddleware(boolean scroll) {
    this.scroll = scroll;
  }

  /** 本调用是否声明 scroll（头值大小写不敏感匹配；未携带=false）。 */
  boolean scroll() {
    return scroll;
  }

  /** 每 RPC 一次：从传入 headers 捕获 scroll 声明。 */
  static final class Factory implements FlightServerMiddleware.Factory<ScrollModeMiddleware> {

    @Override
    public ScrollModeMiddleware onCallStarted(
        CallInfo info, CallHeaders incomingHeaders, RequestContext context) {
      String value = incomingHeaders.get(HEADER);
      return new ScrollModeMiddleware(SCROLL_VALUE.equalsIgnoreCase(value == null ? "" : value.trim()));
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
