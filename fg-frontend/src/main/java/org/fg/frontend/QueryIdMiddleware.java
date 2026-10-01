package org.fg.frontend;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

/**
 * D28 显式续传头捕获：{@code x-fg-query-id}（不透明串，fork 驱动铸造）。
 *
 * <p>语义（与 orchestrator.register 的 ①②③ 对应）：非空且 ≤128 字符 → 显式执行意图管理
 * （命中=续传既有行、未命中=新建执行且该 id 即行身份）；缺失/超长 → 无头路径（现行指纹
 * 去重，pyarrow/ADBC 零变化）。超长视作缺失而非报错——防御面在 orchestrator 的存储边界。
 */
final class QueryIdMiddleware implements FlightServerMiddleware {

  static final String HEADER = "x-fg-query-id";
  private static final int MAX_LENGTH = 128;

  static final FlightServerMiddleware.Key<QueryIdMiddleware> KEY =
      FlightServerMiddleware.Key.of("fg-query-id");

  private final String queryId;

  private QueryIdMiddleware(String queryId) {
    this.queryId = queryId;
  }

  /** 有效头值（null = 未携带/非法 → 无头路径）。 */
  String queryId() {
    return queryId;
  }

  static final class Factory implements FlightServerMiddleware.Factory<QueryIdMiddleware> {

    @Override
    public QueryIdMiddleware onCallStarted(
        CallInfo info, CallHeaders incomingHeaders, RequestContext context) {
      String value = incomingHeaders.get(HEADER);
      if (value != null) {
        value = value.trim();
        if (value.isEmpty() || value.length() > MAX_LENGTH) {
          value = null;
        }
      }
      return new QueryIdMiddleware(value);
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
