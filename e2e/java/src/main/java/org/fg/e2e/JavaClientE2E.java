package org.fg.e2e;

import java.util.concurrent.TimeUnit;
import org.apache.arrow.flight.CancelFlightInfoRequest;
import org.apache.arrow.flight.CancelFlightInfoResult;
import org.apache.arrow.flight.CancelStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * FG M1 e2e（Java 侧）：arrow-java 官方 {@link FlightSqlClient}（JDBC 驱动同源实现）。
 *
 * <p>定位：与 e2e/run_e2e.py（wire 层手工客户端）互补——验证规范 Java 客户端的行为面：
 * Basic 握手、GetFlightInfo 快返 → DoGet 等待、endpoints 遍历、CancelFlightInfo。
 *
 * <p>注意 arrow-java 客户端无 PollFlightInfo API——JDBC 系客户端完全依赖 GetFlightInfo
 * 语义（快返 STREAM 票 + DoGet 内联等待），legacy-long 用例即验证 STREAM 票等待预算
 * （= fg.query.timeout）对此类客户端的必要性。
 *
 * <p>用法：{@code java -jar target/fg-e2e-java-0.1.0-SNAPSHOT.jar {legacy|legacy-long|cancel}}
 * （网关需默认 relay 模式；各用例间 TRUNCATE fg_operation 防 fingerprint 冲突）
 */
public final class JavaClientE2E {

  private static final String HOST = "localhost";
  private static final int PORT = 32010;
  private static final String USER = "fg";
  private static final String PASSWORD = "fg";

  public static void main(String[] args) throws Exception {
    String which = args.length > 0 ? args[0] : "legacy";
    try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        FlightClient client =
            FlightClient.builder(alloc, Location.forGrpcInsecure(HOST, PORT)).build()) {
      client.authenticateBasic(USER, PASSWORD);
      try (FlightSqlClient sql = new FlightSqlClient(client)) {
        switch (which) {
          case "legacy" -> readAll(sql,
              "SELECT id, id * 2 AS dbl FROM range(1000)", 1000, "[java-legacy]");
          case "legacy-long" -> readAll(sql,
              "SELECT id % 1000 AS k, count(*) AS cnt FROM range(20000000) GROUP BY id % 1000",
              1000, "[java-legacy-long]");
          case "cancel" -> cancel(sql);
          default -> throw new IllegalArgumentException("unknown case " + which);
        }
      }
    }
  }

  /** GetFlightInfo 快返 STREAM 票 → 逐 endpoint DoGet（查询在途时挂满全程）→ 断言行数。 */
  private static void readAll(FlightSqlClient sql, String query, long expected, String tag) {
    long t0 = System.nanoTime();
    FlightInfo info = sql.execute(query);
    String qid = info.getAppMetadata() == null
        ? "?" : new String(info.getAppMetadata());
    System.out.printf("%s endpoints=%d queryId=%s...%n", tag, info.getEndpoints().size(),
        qid.substring(0, Math.min(8, qid.length())));
    long total = 0;
    for (FlightEndpoint ep : info.getEndpoints()) {
      try (FlightStream fs = sql.getStream(ep.getTicket())) {
        while (fs.next()) {
          total += fs.getRoot().getRowCount();
        }
      } catch (Exception e) {
        throw new IllegalStateException("getStream failed at " + total + " rows", e);
      }
    }
    if (total != expected) {
      throw new AssertionError(tag + " rows " + total + " != " + expected);
    }
    System.out.printf("%s PASS (%.1fs)%n", tag, elapsedSec(t0));
  }

  /** 在途取消：execute 提交大基数 join → 3s 后 CancelFlightInfo → 断言 CANCELLED。 */
  private static void cancel(FlightSqlClient sql) throws InterruptedException {
    long t0 = System.nanoTime();
    FlightInfo info = sql.execute(
        "SELECT a.id FROM range(400000000) a JOIN range(500) b ON a.id % 500 = b.id");
    System.out.printf("[java-cancel] queryId=%s... submitted%n",
        new String(info.getAppMetadata()).substring(0, 8));
    Thread.sleep(3_000);
    CancelFlightInfoResult result = sql.cancelFlightInfo(new CancelFlightInfoRequest(info));
    CancelStatus status = result.getStatus();
    System.out.printf("[java-cancel] CancelStatus=%s%n", status);
    if (status != CancelStatus.CANCELLED) {
      throw new AssertionError("expected CANCELLED, got " + status);
    }
    System.out.printf("[java-cancel] PASS (%.1fs)%n", elapsedSec(t0));
  }

  private static double elapsedSec(long t0) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) / 1000.0;
  }

  private JavaClientE2E() {}
}
