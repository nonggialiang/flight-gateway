package org.fg.frontend;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

/**
 * D38：inline 断线续传游标捕获（照 PagingMiddleware 范式）——DoGet 请求头
 * {@code x-fg-resume-response-id} 携带客户端最后收到的 responseId（来源：上一流每个
 * FlightData 的 app_metadata）；缺省 = 从头消费。头不进 HMAC（票本授权全量读，游标只
 * 收窄起点，同 D27 offset/limit 先例）。非法值（空白/超长）按缺省处理（不阻断哲学）。
 */
final class ResumeMiddleware implements FlightServerMiddleware {

  static final String HEADER = "x-fg-resume-response-id";
  static final FlightServerMiddleware.Key<ResumeMiddleware> KEY =
      FlightServerMiddleware.Key.of("fg-resume");

  private final String resumeResponseId;

  private ResumeMiddleware(String resumeResponseId) {
    this.resumeResponseId = resumeResponseId;
  }

  String resumeResponseId() {
    return resumeResponseId;
  }

  static final class Factory implements FlightServerMiddleware.Factory<ResumeMiddleware> {

    @Override
    public ResumeMiddleware onCallStarted(
        CallInfo info, CallHeaders incomingHeaders, RequestContext context) {
      String value = incomingHeaders.get(HEADER);
      if (value == null) {
        return new ResumeMiddleware(null);
      }
      String trimmed = value.trim();
      if (trimmed.isEmpty() || trimmed.length() > 256) {
        return new ResumeMiddleware(null); // 非法值按从头消费（不阻断）
      }
      return new ResumeMiddleware(trimmed);
    }
  }

  // 纯捕获型 middleware

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {}

  @Override
  public void onCallCompleted(CallStatus status) {}

  @Override
  public void onCallErrored(Throwable error) {}
}
