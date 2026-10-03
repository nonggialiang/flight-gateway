package org.fg.e2e;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * fg-p5（D30）默认连接"每次 execute 必新查询"验证探针：
 *
 * <p>① 同一 statement 对象重执行同 SQL——两次数据正确且 {@code fg_query_registered_total}
 * 走动 ≥2（两次真实建行，非指纹复用单行）；② DML 后重查<b>同文本</b> SELECT——读到新数据
 * （读己之写；fg-p5 前会命中指纹终态复用拿到旧快照）；③ 取消——SQLException 直达应用
 * （message 为 "Error while executing SQL..."，非重试耗尽的 "Failed ... after N attempts"）
 * 且 cause 链含 CANCELLED，行未流尽（终态失败零重试，绝不复活已取消查询）。
 */
public final class FreshExecuteProbe {

  public static void main(String[] args) throws Exception {
    String url = "jdbc:arrow-flight://localhost:32010?useEncryption=false";

    // ① 同 statement 重执行 = 两次新执行
    try (Connection conn = DriverManager.getConnection(url, props());
        Statement st = conn.createStatement()) {
      long before = metricQueryRegistered();
      int n1 = count(st.executeQuery("SELECT id FROM range(100)"));
      int n2 = count(st.executeQuery("SELECT id FROM range(100)")); // 同一 statement 对象
      check(n1 == 100 && n2 == 100, "rows " + n1 + "/" + n2);
      long delta = metricQueryRegistered() - before;
      check(before < 0 || delta >= 2,
          "fg_query_registered_total delta=" + delta + "（期望 ≥2）");
    }
    System.out.println("[fresh] ① 同 statement 重执行=两次新执行（registered≥2）OK");

    // ② DML 后重查同文本 = 新数据（读己之写）
    // 注：引擎无 Hive 支持——CREATE [OR REPLACE] TABLE AS SELECT 必须显式 USING parquet
    //（实证：REPLACE TABLE AS SELECT / CREATE Hive TABLE 均报 NOT_SUPPORTED）
    try (Connection conn = DriverManager.getConnection(url, props());
        Statement st = conn.createStatement()) {
      st.execute("DROP TABLE IF EXISTS fresh_probe_t");
      st.execute("CREATE TABLE fresh_probe_t USING parquet AS SELECT id FROM range(10)");
      long c1 = scalar(st, "SELECT COUNT(*) FROM fresh_probe_t");
      st.execute("INSERT INTO fresh_probe_t SELECT id + 10 FROM fresh_probe_t");
      long c2 = scalar(st, "SELECT COUNT(*) FROM fresh_probe_t"); // 与 c1 完全同文本
      st.execute("DROP TABLE IF EXISTS fresh_probe_t");
      check(c1 == 10 && c2 == 20, "counts " + c1 + "/" + c2 + "（期望 10/20；旧值 10/10=指纹复用旧快照）");
    }
    System.out.println("[fresh] ② DML 后重查同文本=新数据（读己之写）OK");

    // ③ 取消：终态失败直达（零重试），cause 链 CANCELLED，行未流尽
    long t0 = System.nanoTime();
    try (Connection conn = DriverManager.getConnection(url, props());
        Statement st = conn.createStatement()) {
      final boolean[] done = {false};
      Thread killer = new Thread(() -> {
        try {
          for (int i = 0; !done[0] && i < 600; i++) {
            Thread.sleep(1_000);
            try {
              st.cancel();
            } catch (SQLException ignored) {
              // statement 可能已关闭
            }
          }
        } catch (InterruptedException ignored) {
          // 线程退场
        }
      });
      killer.start();
      long total = 0;
      boolean failed = false;
      String message = "";
      try (ResultSet rs = st.executeQuery(
          "SELECT a.id FROM range(400000000) a JOIN range(500) b ON a.id % 500 = b.id")) {
        while (rs.next()) {
          total++;
        }
      } catch (SQLException e) {
        failed = true;
        message = chainToString(e);
      }
      done[0] = true;
      killer.join();
      double sec = (System.nanoTime() - t0) / 1_000_000_000.0;
      check(failed, "取消未生效：流尽 " + total + " 行");
      check(message.contains("CANCELLED") || message.contains("Query was cancelled"),
          "cause 链无 CANCELLED：" + message);
      check(message.contains("Error while executing SQL"),
          "非直达路径（疑似重试耗尽包装）：" + message);
      check(!message.contains("after 5 attempts"), "触发了 Avatica 重试耗尽：" + message);
      System.out.printf("[fresh] ③ 取消直达应用 %.1fs（零重试，cause 含 CANCELLED）OK%n", sec);
    }
    System.out.println("[fresh] PASS");
  }

  private static int count(ResultSet rs) throws SQLException {
    int n = 0;
    try (rs) {
      while (rs.next()) {
        n++;
      }
    }
    return n;
  }

  private static long scalar(Statement st, String sql) throws SQLException {
    try (ResultSet rs = st.executeQuery(sql)) {
      return rs.next() ? rs.getLong(1) : -1;
    }
  }

  private static String chainToString(Throwable t) {
    StringBuilder sb = new StringBuilder(String.valueOf(t.getMessage()));
    Throwable c = t.getCause();
    int depth = 0;
    while (c != null && depth++ < 8) {
      sb.append(" <- ").append(c.getClass().getSimpleName())
          .append(':').append(c.getMessage());
      c = c.getCause();
    }
    return sb.toString();
  }

  /** fg_query_registered_total{kind="QUERY"} 当前值（本机 :9091；不可达返回 -1）。 */
  private static long metricQueryRegistered() {
    try {
      java.io.BufferedReader r =
          new java.io.BufferedReader(
              new java.io.InputStreamReader(
                  new java.net.URL("http://localhost:9091/metrics").openStream()));
      String line;
      while ((line = r.readLine()) != null) {
        if (line.startsWith("fg_query_registered_total{kind=\"QUERY\"")) {
          return (long) Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
        }
      }
    } catch (Exception e) {
      System.out.println("[fresh] metrics unavailable: " + e);
    }
    return -1;
  }

  private static void check(boolean ok, String what) {
    if (!ok) {
      throw new AssertionError(what);
    }
  }

  private static Properties props() {
    Properties p = new Properties();
    p.setProperty("user", "fg");
    p.setProperty("password", "fg");
    return p;
  }

  private FreshExecuteProbe() {}
}
