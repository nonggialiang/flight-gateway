package org.fg.e2e;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * D28 显式续传全链路探针（DBeaver 形态模拟：statementDefaultScroll + setMaxRows 段信号）。
 *
 * <p>① 段式续传：同 SQL、maxRows 200 → 400（DBeaver setLimit 泄漏信号）→ 第二次执行续传
 * （同 queryId、引擎不跑、数据正确、网关页 DoGet 恰好 segment 次——读时加载下 absolute
 * 跳行零拉页）。② 新执行：同 SQL、maxRows 回 200（≤ lastMax）→ 重新执行（新 queryId）。
 * ③ 显式 nonce：不同 SQL → 新执行。④ executeUpdate 后清槽 → 同 SQL 再执行 = 新执行。
 */
public final class ContinuationProbe {
  public static void main(String[] args) throws Exception {
    String url = "jdbc:arrow-flight://localhost:32010?useEncryption=false&statementDefaultScroll=true";

    try (Connection conn = DriverManager.getConnection(url, props())) {
      // ① 首段（maxRows=200 = DBeaver 首块）
      readSegment(conn, "SELECT id, id * 2 AS dbl FROM range(500) ORDER BY id", 200, 0, 200);
      long pages1 = fetchCountMetric();

      // 续传段（maxRows=400 = offset 200 + segment 200）
      readSegment(conn, "SELECT id, id * 2 AS dbl FROM range(500) ORDER BY id", 400, 200, 200);
      long pages2 = fetchCountMetric();
      System.out.println("[cont] ① 续传段网关页 DoGet = " + (pages2 - pages1)
          + "（期望 ≤2：200 行新段恰 1 页 + 顺序读零浪费；读时加载）");

      // ② 同 SQL maxRows 回 200（≤ lastMax 400）→ 新执行
      String qidBefore = lastQueryId();
      readSegment(conn, "SELECT id, id * 2 AS dbl FROM range(500) ORDER BY id", 200, 0, 200);
      String qidAfter = lastQueryId();
      System.out.println("[cont] ② maxRows 回落 → 新执行：queryId 变化 = " + !qidBefore.equals(qidAfter));

      // ③ 不同 SQL → 新执行
      readSegment(conn, "SELECT id FROM range(10) ORDER BY id", 10, 0, 10);
      System.out.println("[cont] ③ 不同 SQL 新执行 OK");

      // ④ executeUpdate 清槽
      try (Statement st = conn.createStatement()) {
        st.execute("CREATE TABLE IF NOT EXISTS fg_cont_probe (id INT) USING PARQUET");
        st.execute("INSERT INTO fg_cont_probe VALUES (1)");
      }
      readSegment(conn, "SELECT id, id * 2 AS dbl FROM range(500) ORDER BY id", 200, 0, 200);
      System.out.println("[cont] ④ executeUpdate 后同 SQL = 新执行 OK（清槽）");
      try (Statement st = conn.createStatement()) {
        st.execute("DROP TABLE IF EXISTS fg_cont_probe");
      }
    }
    System.out.println("[cont] PASS");
  }

  /** 读 [from, from+count) 行（模拟 DBeaver：setMaxRows(N) + 绝对定位 + 顺序 next）。 */
  private static void readSegment(Connection conn, String sql, int maxRows, int from, int count)
      throws Exception {
    try (Statement st = conn.createStatement()) {
      st.setMaxRows(maxRows);
      try (ResultSet rs = st.executeQuery(sql)) {
        if (from > 0) {
          if (!rs.absolute(from)) {
            throw new AssertionError("absolute(" + from + ") failed");
          }
        }
        long expect = from;
        int got = 0;
        while (rs.next() && got < count) {
          if (rs.getLong(1) != expect) {
            throw new AssertionError("row " + (from + got) + ": " + rs.getLong(1) + " != " + expect);
          }
          if (sql.contains("dbl") && rs.getLong(2) != expect * 2) {
            throw new AssertionError("dbl mismatch at " + expect);
          }
          expect++;
          got++;
        }
        if (got != count) {
          throw new AssertionError("got " + got + " != " + count);
        }
      }
    }
  }

  private static String lastQueryId() throws Exception {
    java.io.BufferedReader r = new java.io.BufferedReader(
        new java.io.InputStreamReader(Runtime.getRuntime().exec(new String[] {
            "docker", "exec", "fg-pg", "psql", "-U", "fg", "-d", "flightgateway", "-tAc",
            "SELECT query_id FROM fg_operation ORDER BY created_at DESC, query_id DESC LIMIT 1"
        }).getInputStream()));
    return r.readLine();
  }

  private static long fetchCountMetric() {
    try {
      java.io.BufferedReader r =
          new java.io.BufferedReader(
              new java.io.InputStreamReader(
                  new java.net.URL("http://localhost:9091/metrics").openStream()));
      String line;
      while ((line = r.readLine()) != null) {
        if (line.startsWith("fg_scroll_pages_total")) {
          return (long) Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
        }
      }
    } catch (Exception e) {
      System.out.println("[cont] metrics unavailable: " + e);
    }
    return -1;
  }

  private static Properties props() {
    Properties p = new Properties();
    p.setProperty("user", "fg");
    p.setProperty("password", "fg");
    return p;
  }
}
