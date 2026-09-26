package org.fg.spi;

/**
 * gateway 侧会话上下文（Flight 客户端会话的引擎侧投影，D20 生命周期绑定）。
 *
 * <p><b>sessionId 即引擎（Connect）会话 id，零映射</b>：客户端自报的会话身份（FG JDBC
 * 驱动每连接自动生成 / pyarrow/ADBC 经 x-fg-session-id 头）被入口强制为 UUID 格式
 * （Connect INVALID_HANDLE.FORMAT 的约束前移），原样透传引擎；化身与生命周期事实由
 * fg_session 登记表裁决。
 *
 * @param sessionId fg 会话引用（= Connect session id，UUID），fg_session 主键
 * @param user      认证用户（会话归属；born 冲突路径断言一致，防跨用户占用）
 */
public record GatewaySession(String sessionId, String user) {
  public GatewaySession {
    if (sessionId == null || sessionId.isBlank()) {
      throw new IllegalArgumentException("sessionId required");
    }
    if (user == null || user.isBlank()) {
      throw new IllegalArgumentException("user required");
    }
  }
}
