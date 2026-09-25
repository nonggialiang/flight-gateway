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
 * {legacy|legacy-long|metadata|cancel|cancel-poll|mode}}（网关默认 relay 模式；cancel-poll 需
 * {@code -Dfg.poll.max-wait=2s}；用例间 TRUNCATE fg_operation）。
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
        case "mode" -> modeUrl(props);
        case "cancel" -> cancel(conn);
        case "cancel-poll" -> cancelPoll(conn);
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

  /**
   * D15 mode 协商（JDBC 通道）：非内建连接属性经驱动 {@code toCallOption()} 透传为 RPC 头
   * （URL 参数与 Properties 等价，均汇入 Avatica ConnectionConfig）→ 网关按头以 https 模式
   * 落行（fg_operation.mode=HTTPS，终态 endpoints=presigned URL，驱动走 openEndpoint HTTP
   * 取数）。SQL 与 legacy 案例区分：fingerprint 幂等（user+sql）下同 SQL 复用在册行
   * （mode 首注册落行，在册行优先）。
   */
  private static void modeUrl(Properties props) throws Exception {
    long t0 = System.nanoTime();
    String url = URL + "&x-fg-endpoint-mode=https";
    try (Connection conn = DriverManager.getConnection(url, props);
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT id, id * 5 AS quint FROM range(1000)")) {
      long total = 0;
      while (rs.next()) {
        total++;
      }
      if (total != 1000) {
        throw new AssertionError("[jdbc-mode] rows " + total + " != 1000");
      }
    }
    System.out.printf("[jdbc-mode] PASS (%.1fs)%n", elapsedSec(t0));
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

  /**
   * 在途取消（fg-p1 poll 链路下）：executeQuery 阻塞在 PreparedStatement.execute 的 poll
   * 轮询直至物化完成（此阶段 Avatica cancel 是 no-op——openResultSet 尚未建立），随后
   * rs.next() 进入取数；killer 线程循环 st.cancel()（结果集出现后即生效——Avatica 层
   * 抛 "Statement canceled"）。未取消时全量 50M 行远超阈值即可判失败。
   */
  private static void cancel(Connection conn) throws Exception {
    long t0 = System.nanoTime();
    try (Statement st = conn.createStatement()) {
      final boolean[] done = {false};
      Thread killer = new Thread(() -> {
        try {
          // 覆盖 poll 阻塞期（no-op）与取数期（生效）；每秒重试直至用例结束
          for (int i = 0; !done[0] && i < 600; i++) {
            Thread.sleep(1_000);
            try {
              st.cancel();
            } catch (SQLException ignored) {
              // statement 可能已关闭
            }
          }
        } catch (Exception ignored) {
          // 线程中断等
        }
      });
      killer.start();
      long total = 0;
      try (ResultSet rs = st.executeQuery(
          "SELECT a.id FROM range(50000000) a JOIN range(10) b ON a.id % 10 = b.id")) {
        while (rs.next()) {
          total++;
        }
        System.out.printf("[jdbc-cancel] stream ended cleanly with %d rows (expect cancel)%n", total);
      } catch (SQLException e) {
        System.out.printf("[jdbc-cancel] SQLException after %.1fs: %s%n",
            elapsedSec(t0), e.getMessage());
        total = -1; // 取消到达的预期路径
      }
      done[0] = true;
      killer.join();
      if (total > 1_000_000) {
        throw new AssertionError("cancel not effective: " + total + " rows streamed");
      }
      System.out.println("[jdbc-cancel] PASS");
    }
  }

  /**
   * 方案②验证（poll 阻塞期取消 → CancelFlightInfo）：fg-p1-patch② 给驱动接线了
   * Statement.cancel() → CancelFlightInfo——首 poll 返回时凭证（FlightInfo.appMetadata=queryId）
   * 经 listener 存入 statement，阻塞轮询期间的 st.cancel() 即触发服务端取消，随后 poll 收到
   * CANCELLED 以 SQLException 浮出。需 {@code -Dfg.poll.max-wait=2s} 网关（缩短首 poll 服务端
   * 等待，凭证 ~2s 即可达）；400M join 物化远超 3s，保证取消落在 poll 阻塞期（早于任何取数）。
   */
  private static void cancelPoll(Connection conn) throws Exception {
    long t0 = System.nanoTime();
    try (Statement st = conn.createStatement()) {
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
        } catch (Exception ignored) {
          // 线程中断等
        }
      });
      killer.start();
      long total = 0;
      try (ResultSet rs = st.executeQuery(
          "SELECT a.id FROM range(400000000) a JOIN range(500) b ON a.id % 500 = b.id")) {
        while (rs.next()) {
          total++;
        }
        System.out.printf("[jdbc-cancel-poll] stream ended with %d rows (cancel landed late)%n", total);
      } catch (SQLException e) {
        System.out.printf("[jdbc-cancel-poll] SQLException after %.1fs: %s (cause: %s)%n",
            elapsedSec(t0), e.getMessage(),
            e.getCause() == null ? "none" : String.valueOf(e.getCause().getMessage()));
        total = -1; // 取消到达的预期路径
      }
      done[0] = true;
      killer.join();
      if (total > 1_000_000) {
        throw new AssertionError("cancel not effective: " + total + " rows streamed");
      }
      System.out.println("[jdbc-cancel-poll] PASS");
    }
  }

  private static double elapsedSec(long t0) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) / 1000.0;
  }

  private JdbcClientE2E() {}
}
