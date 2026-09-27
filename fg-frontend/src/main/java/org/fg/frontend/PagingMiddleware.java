package org.fg.frontend;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

/**
 * D27 页头捕获（DoGet 时效，EndpointModeMiddleware 同范式）：驱动翻页时对 STREAM 票的
 * DoGet 附 {@code x-fg-page-offset}/{@code x-fg-page-limit} 头——票是能力凭证（STREAM 票
 * 本就授权全量读，页参数只收窄输出），offset/limit 不进 HMAC。
 *
 * <p>语义：<b>offset 头出现即页请求</b>；limit 缺省用服务端默认（fg.result.page.default-rows）。
 * 非法值（不可解析）按未携带处理——该 DoGet 回落全量顺序流（与 EndpointMode 的
 * "非法值不阻断"哲学一致）。Flight 协议的 CallContext 是服务端派生字段（protocol/peer），
 * 客户端写不进去——per-RPC 客户端通道只有 headers，故走本 middleware。
 */
final class PagingMiddleware implements FlightServerMiddleware {

  static final String OFFSET_HEADER = "x-fg-page-offset";
  static final String LIMIT_HEADER = "x-fg-page-limit";

  static final FlightServerMiddleware.Key<PagingMiddleware> KEY =
      FlightServerMiddleware.Key.of("fg-paging");

  /** 页请求参数（offset 非 null 即页请求；limit null = 服务端默认）。 */
  record PageRequest(Long offset, Long limit) {}

  private final PageRequest request;

  private PagingMiddleware(PageRequest request) {
    this.request = request;
  }

  PageRequest request() {
    return request;
  }

  static final class Factory implements FlightServerMiddleware.Factory<PagingMiddleware> {

    @Override
    public PagingMiddleware onCallStarted(
        CallInfo info, CallHeaders incomingHeaders, RequestContext context) {
      Long offset = parseLong(incomingHeaders.get(OFFSET_HEADER));
      Long limit = parseLong(incomingHeaders.get(LIMIT_HEADER));
      return new PagingMiddleware(offset == null ? null : new PageRequest(offset, limit));
    }

    private static Long parseLong(String value) {
      if (value == null) {
        return null;
      }
      try {
        long v = Long.parseLong(value.trim());
        return v >= 0 ? v : null; // 负值按未携带
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  // 纯捕获型 middleware

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {}

  @Override
  public void onCallCompleted(CallStatus status) {}

  @Override
  public void onCallErrored(Throwable err) {}
}
