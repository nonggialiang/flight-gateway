package org.fg.frontend;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RPC 级调试日志（D27 后补，调用顺序分析用）：每个进入 producer 的 RPC（GetFlightInfo/
 * PollFlightInfo/DoGet/DoAction/Handshake/…）各打一行 DEBUG——方法名、peer 身份、
 * 会话身份（cookie 或 x-fg-session-id 头）、其余 fg 自定义头（endpoint-mode/
 * result-set-type/page-*）。排查"哪个 RPC 没带头"一眼可见。
 *
 * <p>会话身份读取与 {@code FgFlightProducer.sessionRef} 同源（cookie 优先、自报头次之），
 * 但<b>只读不裁决</b>——缺失记 {@code NO-SESSION}，不抛（否则只会在日志里看到同样的
 * INVALID_ARGUMENT 而丢失上下文）。
 */
final class RpcTraceMiddleware implements FlightServerMiddleware {

  private static final Logger LOGGER = LoggerFactory.getLogger(RpcTraceMiddleware.class);

  static final FlightServerMiddleware.Key<RpcTraceMiddleware> KEY =
      FlightServerMiddleware.Key.of("fg-rpc-trace");

  private final String line;

  private RpcTraceMiddleware(String line) {
    this.line = line;
  }

  String line() {
    return line;
  }

  static final class Factory implements FlightServerMiddleware.Factory<RpcTraceMiddleware> {

    @Override
    public RpcTraceMiddleware onCallStarted(
        CallInfo info, CallHeaders incomingHeaders, RequestContext context) {
      StringBuilder sb = new StringBuilder(128);
      sb.append("rpc=").append(info.method());
      // 会话身份（自报头；middleware 阶段拿不到 peer——producer 层日志补）
      String declared = incomingHeaders.get(SessionIdMiddleware.HEADER);
      sb.append(" session=").append(declared != null ? declared : "NO-SESSION");
      // 其余 fg 自定义头（协商/翻页），不存在则省略
      appendIfPresent(sb, incomingHeaders, EndpointModeMiddleware.HEADER);
      appendIfPresent(sb, incomingHeaders, ScrollModeMiddleware.HEADER);
      appendIfPresent(sb, incomingHeaders, PagingMiddleware.OFFSET_HEADER);
      appendIfPresent(sb, incomingHeaders, PagingMiddleware.LIMIT_HEADER);
      String line = sb.toString();
      LOGGER.debug(line);
      return new RpcTraceMiddleware(line);
    }

    private static void appendIfPresent(StringBuilder sb, CallHeaders headers, String header) {
      String value = headers.get(header);
      if (value != null) {
        sb.append(' ').append(header).append('=').append(value);
      }
    }
  }

  // 纯观测型 middleware

  @Override
  public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {}

  @Override
  public void onCallCompleted(CallStatus status) {}

  @Override
  public void onCallErrored(Throwable err) {}
}
