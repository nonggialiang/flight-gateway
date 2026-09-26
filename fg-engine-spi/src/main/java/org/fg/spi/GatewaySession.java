package org.fg.spi;

/**
 * gateway 侧会话上下文（Flight 客户端会话的引擎侧投影，D20 生命周期绑定）。
 *
 * @param sessionId       fg 会话引用（cookie id / x-fg-session-id 头），fg_session 主键
 * @param user            认证用户
 * @param connectSessionId 会话登记表铸造的引擎（Connect）会话 id——网关不再确定性派生，
 *                        化身（incarnation）由登记表唯一裁决
 */
public record GatewaySession(String sessionId, String user, String connectSessionId) {
  public GatewaySession {
    if (sessionId == null || sessionId.isBlank()) {
      throw new IllegalArgumentException("sessionId required");
    }
    if (connectSessionId == null || connectSessionId.isBlank()) {
      throw new IllegalArgumentException("connectSessionId required (minted by session registry)");
    }
  }
}
