package org.fg.e2e;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * FG M1 e2e（JDBC 侧）：arrow 官方 {@code flight-sql-jdbc-driver}（Avatica 基座）。
 *
 * <p>定位：第三路客户端——真实 BI/分析工具走 JDBC 的形态。覆盖：Basic 连接、
 * DatabaseMetaData/SqlInfo（CommandGetSqlInfo）、executeQuery（GetFlightInfo 快返 →
 * DoGet 内联等待）、{@link Statement#cancel()}（→ CancelFlightInfo）。
 *
 * <p>用法：{@code java -cp target/fg-e2e-java-0.1.0-SNAPSHOT.jar org.fg.e2e.JdbcClientE2E
 * {legacy|legacy-long|metadata|cancel}}（网关默认 relay 模式；用例间 TRUNCATE fg_operation）。
 */
public final class JdbcClientE2E {

  // 驱动默认 useEncryption=true（TLS），M1 网关为明文端口须显式关闭（M3 接 TLS 后移除）
  private static final String URL = "jdbc:arrow-flight://localhost:32010?useEncryption=false";

  public static void main(String[] args) throws Exception {
    String which = args.length > 0 ? args[0] : "legacy";
    Properties props = new Properties();
    props.setProperty("user", "fg");
    props.setProperty("password", "fg");
    try (Connection conn = DriverManager.getConnection(URL, props)) {
      switch (which) {
        case "legacy" -> query(conn,
            "SELECT id, id * 2 AS dbl FROM range(1000)", 1000, "[jdbc-legacy]");
        case "legacy-long" -> query(conn,
            "SELECT id % 1000 AS k, count(*) AS cnt FROM range(20000000) GROUP BY id % 1000",
            1000, "[jdbc-legacy-long]");
        case "metadata" -> metadata(conn);
        case "cancel" -> cancel(conn);
        default -> throw new IllegalArgumentException("unknown case " + which);
      }
    }
  }

  private static void query(Connection conn, String sql, long expected, String tag)
      throws Exception {
    long t0 = System.nanoTime();
    try (Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(sql)) {
      long total = 0;
      while (rs.next()) {
        total++;
      }
      if (total != expected) {
        throw new AssertionError(tag + " rows " + total + " != " + expected);
      }
      System.out.printf("%s PASS (%.1fs)%n", tag, elapsedSec(t0));
    }
  }

  /** 触发 CommandGetSqlInfo（FgSqlInfoProvider 最小面）。 */
  private static void metadata(Connection conn) throws Exception {
    System.out.printf("[jdbc-meta] productName=%s driverName=%s url=%s%n",
        conn.getMetaData().getDatabaseProductName(),
        conn.getMetaData().getDriverName(),
        conn.getMetaData().getURL());
    System.out.printf("[jdbc-meta] isReadOnly=%s%n", conn.isReadOnly());
    System.out.println("[jdbc-meta] PASS");
  }

  /** 在途取消：executeQuery 快返 → rs.next() 挂在 DoGet 等待 → 3s 后 st.cancel() → CancelFlightInfo。 */
  private static void cancel(Connection conn) throws Exception {
    long t0 = System.nanoTime();
    try (Statement st = conn.createStatement()) {
      Thread killer = new Thread(() -> {
        try {
          Thread.sleep(3_000);
          st.cancel();
        } catch (Exception ignored) {
          // statement 可能已关闭
        }
      });
      killer.start();
      long total = 0;
      try (ResultSet rs = st.executeQuery(
          "SELECT a.id FROM range(400000000) a JOIN range(500) b ON a.id % 500 = b.id")) {
        while (rs.next()) {
          total++;
        }
        System.out.printf("[jdbc-cancel] stream ended cleanly with %d rows (expect cancel)%n", total);
      } catch (SQLException e) {
        System.out.printf("[jdbc-cancel] SQLException after %.1fs: %s%n",
            elapsedSec(t0), e.getMessage());
        total = -1; // 取消到达的预期路径
      }
      killer.join();
      if (total > 1_000_000) {
        throw new AssertionError("cancel not effective: " + total + " rows streamed");
      }
      System.out.println("[jdbc-cancel] PASS");
    }
  }

  private static double elapsedSec(long t0) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) / 1000.0;
  }

  private JdbcClientE2E() {}
}
