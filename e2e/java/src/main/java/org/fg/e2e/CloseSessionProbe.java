package org.fg.e2e;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;

/**
 * fg-p5 后补（D30）：Connection.close() 无条件 best-effort CloseSession 验证探针。
 *
 * <p>背景：上游 ArrowFlightSqlClientHandler.close() 仅在 catalog 存在时发送 CloseSession，
 * FG 连接不携 catalog → DBeaver 断开后 fg_session 僵尸 ACTIVE + 引擎 Connect 会话滞留。
 *
 * <p>场景：① 钉定 session id 连接执行 SQL（fg_session ACTIVE + 引擎会话登记）；② 断开
 * （驱动补发 CloseSession）→ 引擎 admin /session/status 应 alive=false（Connect 会话逐出）；
 * ③ 同 id 重连 → 粘性拒（fg_session CLOSED(client)）；④ 新 session id 正常（回归）。
 */
public final class CloseSessionProbe {

  private static final String BASE = "jdbc:arrow-flight://localhost:32010?useEncryption=false";

  public static void main(String[] args) throws Exception {
    Properties p = new Properties();
    p.setProperty("user", "fg");
    p.setProperty("password", "fg");

    String sid = UUID.randomUUID().toString();
    String url = BASE + "&x-fg-session-id=" + sid;

    // ① 连接 + 执行（会话诞生，引擎侧 Connect 会话登记）
    try (Connection conn = DriverManager.getConnection(url, p);
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT 42")) {
      check(rs.next() && rs.getInt(1) == 42, "首查失败");
    }
    // ② 上面的 try-with-resources 已触发 close() → CloseSession 已 best-effort 发出
    check(!engineSessionAlive(sid), "引擎 Connect 会话未被逐出（close 后仍 alive）");
    System.out.println("[close] ① 断开后引擎 Connect 会话逐出（alive=false）OK");

    // ③ 同 id 重连 → fg_session CLOSED(client) sticky 拒
    boolean stickyRejected = false;
    String message = "";
    try (Connection conn = DriverManager.getConnection(url, p);
        Statement st = conn.createStatement()) {
      st.executeQuery("SELECT 1");
    } catch (Exception e) {
      stickyRejected = true;
      message = String.valueOf(e.getMessage());
    }
    check(stickyRejected, "同 id 重连未被拒（fg_session 非 CLOSED sticky）：" + message);
    check(message.contains("Session closed") || message.toLowerCase().contains("closed"),
        "拒因非 session closed：" + message);
    System.out.println("[close] ② 同 id 重连 sticky 拒（fg_session CLOSED client）OK: " + message);

    // ④ 新 session id 正常
    try (Connection conn = DriverManager.getConnection(
        BASE + "&x-fg-session-id=" + UUID.randomUUID(), p);
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT 7")) {
      check(rs.next() && rs.getInt(1) == 7, "新会话查询失败");
    }
    System.out.println("[close] ③ 新 session id 正常 OK");
    System.out.println("[close] PASS");
  }

  /** 引擎 admin /session/status 探测（默认固定引擎 15003；不可达按 alive=true 保守失败）。 */
  private static boolean engineSessionAlive(String sessionId) throws Exception {
    HttpURLConnection c =
        (HttpURLConnection) new URL(
            "http://localhost:15003/session/status?user=fg&id=" + sessionId).openConnection();
    c.setConnectTimeout(2000);
    c.setReadTimeout(2000);
    try (BufferedReader r = new BufferedReader(new InputStreamReader(c.getInputStream()))) {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = r.readLine()) != null) {
        sb.append(line);
      }
      return sb.toString().contains("\"alive\":true");
    }
  }

  private static void check(boolean ok, String what) {
    if (!ok) {
      throw new AssertionError(what);
    }
  }

  private CloseSessionProbe() {}
}
