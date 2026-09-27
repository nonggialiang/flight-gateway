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
 * {legacy|legacy-long|metadata|cancel|cancel-poll|mode|types|prepare|update}}（网关默认
 * relay 模式；cancel-poll 需 {@code -Dfg.poll.max-wait=2s}；用例间 TRUNCATE fg_operation）。
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
        case "metadata-browse" -> metadataBrowse(conn);
        case "mode" -> modeUrl(props);
        case "types" -> types(conn);
        case "prepare" -> prepare(conn);
        case "update" -> update(conn);
        case "set" -> set(conn);
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

  /**
   * 类型化取数 accessors（此前用例只数行数从未读值——JDBC 类型映射 bug 恰藏于此）：
   * bigint/double/string/decimal/null/timestamp 六列，逐行逐列断言 getXxx 值、
   * {@link ResultSet#wasNull()}、{@link java.sql.ResultSetMetaData}（列数+列名），并顺带
   * {@link Statement#setFetchSize(int)}（BI 常用，验证不破坏流式取数）。
   *
   * <p>timestamp 列已知怪癖（2026-09-25 实测，D16 记录）：线上是 timestamp[us, tz=UTC]
   * 且 instant 精确（pyarrow 直读 part 文件实证），但上游 arrow JDBC 驱动 accessor 是
   * 墙钟语义——{@code Timestamp.valueOf(UTC 墙钟)} 按 JVM 本地时区重解释，zoned 向量再
   * 经 calendar 偏移调整一次 → UTC+8 客户端读出 epoch 偏 -16h（双重偏移）。FG/引擎/网关
   * 无责；故此处断言行间 delta（恒 86,400,000ms）而非绝对 epoch，绝对值仅打印留痕。
   */
  private static void types(Connection conn) throws Exception {
    long t0 = System.nanoTime();
    try (Statement st = conn.createStatement()) {
      st.setFetchSize(128);
      try (ResultSet rs = st.executeQuery(
          "SELECT id AS c_bigint, id * 1.5 AS c_double,"
              + " concat('v-', cast(id AS STRING)) AS c_str,"
              + " CAST(id AS DECIMAL(10,2)) AS c_dec,"
              + " CAST(NULL AS BIGINT) AS c_null,"
              + " CAST(id * 86400 AS TIMESTAMP) AS c_ts"
              + " FROM range(3) ORDER BY id")) {
        java.sql.ResultSetMetaData md = rs.getMetaData();
        if (md.getColumnCount() != 6) {
          throw new AssertionError("[jdbc-types] columns " + md.getColumnCount() + " != 6");
        }
        String[] labels = {"c_bigint", "c_double", "c_str", "c_dec", "c_null", "c_ts"};
        for (int i = 0; i < labels.length; i++) {
          if (!labels[i].equalsIgnoreCase(md.getColumnLabel(i + 1))) {
            throw new AssertionError("[jdbc-types] label[" + i + "]="
                + md.getColumnLabel(i + 1) + " != " + labels[i]);
          }
        }
        long rows = 0;
        long prevTsEpoch = Long.MIN_VALUE;
        while (rs.next()) {
          long id = rows;
          if (rs.getLong(1) != id) {
            throw new AssertionError("[jdbc-types] row " + id + " getLong=" + rs.getLong(1));
          }
          if (Math.abs(rs.getDouble(2) - id * 1.5) > 1e-9) {
            throw new AssertionError("[jdbc-types] row " + id + " getDouble=" + rs.getDouble(2));
          }
          if (!("v-" + id).equals(rs.getString(3))) {
            throw new AssertionError("[jdbc-types] row " + id + " getString=" + rs.getString(3));
          }
          if (rs.getBigDecimal(4).compareTo(java.math.BigDecimal.valueOf(id).setScale(2)) != 0) {
            throw new AssertionError("[jdbc-types] row " + id + " dec=" + rs.getBigDecimal(4));
          }
          rs.getLong(5);
          if (!rs.wasNull()) {
            throw new AssertionError("[jdbc-types] row " + id + " null col wasNull=false");
          }
          long tsEpoch = rs.getTimestamp(6).getTime();
          System.out.printf("[jdbc-types] row %d ts epoch=%d (driver 墙钟语义，绝对值含时区偏移)%n",
              id, tsEpoch);
          if (prevTsEpoch != Long.MIN_VALUE && tsEpoch - prevTsEpoch != 86_400_000L) {
            throw new AssertionError("[jdbc-types] row " + id + " ts delta="
                + (tsEpoch - prevTsEpoch) + " != 86400000");
          }
          prevTsEpoch = tsEpoch;
          rows++;
        }
        if (rows != 3) {
          throw new AssertionError("[jdbc-types] rows " + rows + " != 3");
        }
      }
    }
    System.out.printf("[jdbc-types] PASS (%.1fs)%n", elapsedSec(t0));
  }

  /**
   * 显式 {@link java.sql.PreparedStatement} 双面：
   *
   * <ul>
   *   <li>正例：prepareStatement + executeQuery 走真 prepared 线（CreatePreparedStatement
   *       垫片 → DoPut 参数批 ack → CommandPreparedStatementQuery），与 legacy 的
   *       Avatica 内部 prepare 等价但显式；
   *   <li>负例：{@code ?} 占位——M1 垫片无参数绑定（M3），prepare 的 plan-only
   *       AnalyzePlan 对未解析占位符即刻失败（fail-fast，SQLException 而非挂起/错读）。
   * </ul>
   */
  private static void prepare(Connection conn) throws Exception {
    long t0 = System.nanoTime();
    try (java.sql.PreparedStatement ps =
        conn.prepareStatement("SELECT id, id * 7 AS sept FROM range(1000)")) {
      try (ResultSet rs = ps.executeQuery()) {
        long total = 0;
        while (rs.next()) {
          total++;
        }
        if (total != 1000) {
          throw new AssertionError("[jdbc-prepare] rows " + total + " != 1000");
        }
      }
    }
    try {
      java.sql.PreparedStatement bad =
          conn.prepareStatement("SELECT id FROM range(10) WHERE id > ?");
      try (ResultSet ignored = bad.executeQuery()) {
        throw new AssertionError("[jdbc-prepare] ? 占位未按预期失败");
      } catch (SQLException e) {
        System.out.printf("[jdbc-prepare] ?-execute SQLException: %s%n", firstLine(e.getMessage()));
      } finally {
        bad.close();
      }
    } catch (SQLException e) {
      System.out.printf("[jdbc-prepare] ?-prepare SQLException: %s%n", firstLine(e.getMessage()));
    }
    System.out.printf("[jdbc-prepare] PASS (%.1fs)%n", elapsedSec(t0));
  }

  /**
   * DDL/DML 正面（D17/D18，SqlInfo 已开 DDL/写）：CREATE/INSERT/DROP 经 executeUpdate，
   * V1 in-memory catalog 下 INSERT 无行产出（update count=0；num_affected_rows 仅 V2
   * write path 有——同 python ddl-dml 用例 pin 的实际形状）；写效应由 count(*) 查询证明。
   */
  private static void update(Connection conn) throws Exception {
    long t0 = System.nanoTime();
    try (Statement st = conn.createStatement()) {
      st.executeUpdate("DROP TABLE IF EXISTS fg_e2e_jdbc_t");
      st.executeUpdate("CREATE TABLE fg_e2e_jdbc_t (id BIGINT) USING PARQUET");
      int affected = st.executeUpdate("INSERT INTO fg_e2e_jdbc_t SELECT id FROM range(10)");
      if (affected != 0) {
        // V2 catalog 接入后这里会变成 10——条件记录，不硬断 0
        System.out.printf("[jdbc-update] INSERT count=%d（V2 catalog 应为 10）%n", affected);
      }
      try (ResultSet rs = st.executeQuery("SELECT count(*) AS n FROM fg_e2e_jdbc_t")) {
        rs.next();
        if (rs.getLong(1) != 10) {
          throw new AssertionError("[jdbc-update] count(*)=" + rs.getLong(1) + " != 10");
        }
      }
      st.executeUpdate("DROP TABLE fg_e2e_jdbc_t");
    }
    System.out.printf("[jdbc-update] PASS (%.1fs)%n", elapsedSec(t0));
  }

  /**
   * D21 元数据浏览（BI 工具通路）：DatabaseMetaData 全族——getCatalogs（动态实名
   * spark_catalog）/getSchemas（pattern）/getTableTypes/getTables（% pattern）/getColumns
   * （includeSchema 链路：getTables(true) + 驱动侧解 table_schema VARBINARY 展开列——JDBC
   * getColumns 的唯一通路）/getPrimaryKeys/getImportedKeys（约束族空结果）。
   */
  private static void metadataBrowse(Connection conn) throws Exception {
    long t0 = System.nanoTime();
    java.sql.DatabaseMetaData md = conn.getMetaData();
    try (Statement st = conn.createStatement()) {
      st.executeUpdate("DROP TABLE IF EXISTS fg_e2e_jdbc_meta_t");
      st.executeUpdate(
          "CREATE TABLE fg_e2e_jdbc_meta_t (id BIGINT, val DOUBLE, name STRING) USING PARQUET");

      try (ResultSet rs = md.getCatalogs()) {
        boolean found = false;
        while (rs.next()) {
          found |= "spark_catalog".equals(rs.getString("TABLE_CAT"));
        }
        if (!found) {
          throw new AssertionError("[jdbc-meta-browse] getCatalogs 未含 spark_catalog");
        }
      }

      try (ResultSet rs = md.getSchemas(null, "def%")) {
        boolean found = false;
        while (rs.next()) {
          found |= "default".equals(rs.getString("TABLE_SCHEM"));
        }
        if (!found) {
          throw new AssertionError("[jdbc-meta-browse] getSchemas(def%) 未含 default");
        }
      }

      try (ResultSet rs = md.getTableTypes()) {
        boolean found = false;
        while (rs.next()) {
          found |= "TABLE".equals(rs.getString("TABLE_TYPE"));
        }
        if (!found) {
          throw new AssertionError("[jdbc-meta-browse] getTableTypes 未含 TABLE");
        }
      }

      try (ResultSet rs = md.getTables(null, null, "%", null)) {
        boolean found = false;
        while (rs.next()) {
          if ("fg_e2e_jdbc_meta_t".equals(rs.getString("TABLE_NAME"))) {
            found = "spark_catalog".equals(rs.getString("TABLE_CAT"))
                && "default".equals(rs.getString("TABLE_SCHEM"))
                && "TABLE".equals(rs.getString("TABLE_TYPE"));
          }
        }
        if (!found) {
          throw new AssertionError("[jdbc-meta-browse] getTables 未命中 (spark_catalog, default, fg_e2e_jdbc_meta_t, TABLE)");
        }
      }

      // getColumns：includeSchema 链路（getTables(true) → table_schema → 客户端展开列）
      java.util.Map<String, String> typeByName = new java.util.LinkedHashMap<>();
      try (ResultSet rs = md.getColumns(null, null, "fg_e2e_jdbc_meta_t", "%")) {
        while (rs.next()) {
          typeByName.put(rs.getString("COLUMN_NAME"), rs.getString("TYPE_NAME"));
        }
      }
      if (!typeByName.equals(java.util.Map.of(
          "id", "BIGINT", "val", "DOUBLE", "name", "VARCHAR"))) {
        throw new AssertionError("[jdbc-meta-browse] getColumns 列/类型: " + typeByName);
      }

      try (ResultSet rs = md.getPrimaryKeys("spark_catalog", "default", "fg_e2e_jdbc_meta_t")) {
        if (rs.next()) {
          throw new AssertionError("[jdbc-meta-browse] getPrimaryKeys 应空");
        }
      }
      try (ResultSet rs = md.getImportedKeys("spark_catalog", "default", "fg_e2e_jdbc_meta_t")) {
        if (rs.next()) {
          throw new AssertionError("[jdbc-meta-browse] getImportedKeys 应空");
        }
      }

      st.executeUpdate("DROP TABLE fg_e2e_jdbc_meta_t");
    }
    System.out.printf("[jdbc-meta-browse] PASS (%.1fs)%n", elapsedSec(t0));
  }

  /** SET 语句经 JDBC（命令通道 D17/D18）：execute 返回 [key,value] 结果集。 */
  private static void set(Connection conn) throws Exception {
    long t0 = System.nanoTime();
    try (Statement st = conn.createStatement()) {
      boolean hasRs = st.execute("SET spark.sql.adaptive.enabled=true");
      if (!hasRs) {
        throw new AssertionError("[jdbc-set] SET 未返回结果集");
      }
      try (ResultSet rs = st.getResultSet()) {
        if (!rs.next()) {
          throw new AssertionError("[jdbc-set] SET 结果集空");
        }
        System.out.printf("[jdbc-set] %s=%s%n", rs.getString(1), rs.getString(2));
      }
    }
    System.out.printf("[jdbc-set] PASS (%.1fs)%n", elapsedSec(t0));
  }

  private static String firstLine(String message) {
    if (message == null) {
      return "(null)";
    }
    int nl = message.indexOf('\n');
    return nl > 0 ? message.substring(0, nl) : message;
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
