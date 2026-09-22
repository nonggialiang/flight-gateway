package org.fg.spi;

import java.util.Map;

/** gateway 侧会话上下文（Flight 客户端会话的引擎侧投影）。 */
public record GatewaySession(String sessionId, String user, Map<String, String> options) {
  public GatewaySession {
    options = options == null ? Map.of() : Map.copyOf(options);
  }
}
