package org.fg.e2e;

import java.util.concurrent.TimeUnit;
import com.google.protobuf.Any;
import org.apache.arrow.flight.CancelFlightInfoRequest;
import org.apache.arrow.flight.CancelFlightInfoResult;
import org.apache.arrow.flight.CancelStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PollInfo;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlSession;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * FG M1 e2e（Java 侧）：arrow-java {@link FlightSqlSession}（fork fg-p2 显式会话对象，
 * 包裹官方 FlightSqlClient）。
 *
 * <p>定位：与 e2e/run_e2e.py（wire 层手工客户端）互补——验证规范 Java 客户端的行为面：
 * Basic 握手、GetFlightInfo 快返 → DoGet 等待、endpoints 遍历、CancelFlightInfo。
 *
 * <p>fg-p1（19.0.0-fg-p1，本地 patch）：execute 全链路改 PollFlightInfo（UNIMPLEMENTED 回退
 * GetFlightInfo），并新增 openEndpoint（空票 + http(s) location → HTTP GET presigned
 * URL）。presign 用例即验证该完整链路（需 https 模式网关）。
 *
 * <p>fg-p2（同 fork）：FlightSqlSession 显式会话——open 铸 UUID、本会话全部 RPC 自动携
 * x-fg-session-id、close 发 CloseSession 烧毁 id；会话边界由使用者掌握（与 client 生命周期
 * 解耦）。本 e2e 全部经 session 对象调用，验证该抽象。
 *
 * <p>用法：{@code java -jar target/fg-e2e-java-0.1.0-SNAPSHOT.jar
 * {legacy|legacy-long|cancel|poll|prepare|presign|schema-only|mode-header}}
 * （legacy/legacy-long/prepare/cancel 默认 relay 模式；presign 需
 * {@code -Dfg.result.endpoint.mode=https}；mode-header 在默认 relay 网关上以请求头翻转为
 * https；schema-only 后断言 fg_operation 0 行；各用例间 TRUNCATE fg_operation 防 fingerprint 冲突）
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
      // auth2（网关 auth2-only；arrow-java Handshake 只走 auth1 handler，与 JDBC 互斥）
      CredentialCallOption credential = client.authenticateBasicToken(USER, PASSWORD)
          .orElseThrow(() -> new IllegalStateException("no credential returned"));
      // fg-p2：显式会话（close 即 CloseSession——id 烧毁，进程退出即关）
      try (FlightSqlSession session = FlightSqlSession.open(client, credential)) {
        switch (which) {
          case "legacy" -> readAll(session,
              "SELECT id, id * 2 AS dbl FROM range(1000)", 1000, "[java-legacy]");
          case "legacy-long" -> readAll(session,
              "SELECT id % 1000 AS k, count(*) AS cnt FROM range(20000000) GROUP BY id % 1000",
              1000, "[java-legacy-long]");
          case "cancel" -> cancel(session);
          case "poll" -> poll(session);
          case "prepare" -> {
            // 复刻 JDBC 驱动路径（驱动即 shade 的 FlightSqlClient.prepare/execute）
            FlightSqlSession.SessionPreparedStatement ps =
                session.prepare("SELECT id, id * 2 AS dbl FROM range(1000)");
            try {
              FlightInfo info = ps.execute();
              streamAll(session, info, 1000, "[java-prepare]");
            } finally {
              ps.close();
            }
          }
          case "presign" -> presign(session);
          case "schema-only" -> schemaOnly(session);
          case "mode-header" -> modeHeader(session);
          default -> throw new IllegalArgumentException("unknown case " + which);
        }
      }
    }
  }

  /** GetFlightInfo 快返 STREAM 票 → 逐 endpoint DoGet（查询在途时挂满全程）→ 断言行数。 */
  private static void readAll(FlightSqlSession session, String query, long expected, String tag) {
    long t0 = System.nanoTime();
    FlightInfo info = session.execute(query);
    streamAll(session, info, expected, tag, t0);
  }

  private static void streamAll(FlightSqlSession session, FlightInfo info, long expected,
      String tag) {
    streamAll(session, info, expected, tag, System.nanoTime());
  }

  private static void streamAll(FlightSqlSession session, FlightInfo info, long expected,
      String tag, long t0) {
    String qid = info.getAppMetadata() == null
        ? "?" : new String(info.getAppMetadata());
    System.out.printf("%s endpoints=%d queryId=%s...%n", tag, info.getEndpoints().size(),
        qid.substring(0, Math.min(8, qid.length())));
    long total = 0;
    for (FlightEndpoint ep : info.getEndpoints()) {
      try (FlightStream fs = session.getStream(ep.getTicket())) {
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

  /**
   * plan-only schema 验证（方案 A）：GetSchema 与 CreatePreparedStatement 都只走 AnalyzePlan，
   * 不建 fg_operation 行、不触发执行（外部断言：TRUNCATE 后跑本用例，{@code fg_operation}
   * 应 0 行），且两路 schema 非空、一致。
   */
  private static void schemaOnly(FlightSqlSession session) throws Exception {
    long t0 = System.nanoTime();
    String query = "SELECT id, id * 2 AS dbl FROM range(1000)";
    FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(
        FlightSql.CommandStatementQuery.newBuilder().setQuery(query).build()).toByteArray());
    Schema viaGetSchema = session.getSchema(descriptor).getSchema();
    if (viaGetSchema == null || viaGetSchema.getFields().isEmpty()) {
      throw new AssertionError("getSchema returned empty schema");
    }
    try (FlightSqlSession.SessionPreparedStatement ps = session.prepare(query)) {
      Schema viaPrepare = ps.getResultSetSchema();
      if (viaPrepare == null || viaPrepare.getFields().isEmpty()) {
        throw new AssertionError("prepare dataset_schema empty");
      }
      if (!viaGetSchema.toString().equals(viaPrepare.toString())) {
        throw new AssertionError(
            "getSchema/prepare schema mismatch:\n" + viaGetSchema + "\n" + viaPrepare);
      }
    }
    System.out.printf("[java-schema-only] PASS getSchema fields=%d (%.1fs)%n",
        viaGetSchema.getFields().size(), elapsedSec(t0));
  }

  /**
   * 在途取消：fg-p1 后 execute 阻塞轮询至终态（本查询 ~10-60s 完成于首 poll 的服务端长等待
   * 内，poll 凭证到手已过晚），故与 python cancel 用例同构——getInfo 快返（legacy 快返路径）
   * 取取消凭证 → 3s 后 CancelFlightInfo → 断言 CANCELLED。
   */
  private static void cancel(FlightSqlSession session) throws InterruptedException {
    long t0 = System.nanoTime();
    byte[] command = com.google.protobuf.Any.pack(
            FlightSql.CommandStatementQuery.newBuilder()
                .setQuery("SELECT a.id FROM range(400000000) a JOIN range(500) b ON a.id % 500 = b.id")
                .build())
        .toByteArray();
    FlightInfo info = session.getInfo(FlightDescriptor.command(command));
    System.out.printf("[java-cancel] queryId=%s... submitted%n",
        new String(info.getAppMetadata()).substring(0, 8));
    Thread.sleep(3_000);
    CancelFlightInfoResult result =
        session.cancelFlightInfo(new CancelFlightInfoRequest(info));
    CancelStatus status = result.getStatus();
    System.out.printf("[java-cancel] CancelStatus=%s%n", status);
    if (status != CancelStatus.CANCELLED) {
      throw new AssertionError("expected CANCELLED, got " + status);
    }
    System.out.printf("[java-cancel] PASS (%.1fs)%n", elapsedSec(t0));
  }

  /**
   * fg-p1 presign 完整链路（需 https 模式网关）：execute（patched=poll）轮询至终态 →
   * 断言 endpoints 空票 + http(s) location（presigned URL）→ 逐 endpoint openEndpoint
   * HTTP GET 逐批计行，断言 1000。等价 python https 用例的 Java 侧。
   */
  private static void presign(FlightSqlSession session) throws Exception {
    long t0 = System.nanoTime();
    FlightInfo info = session.execute("SELECT id, id * 2 AS dbl FROM range(1000)");
    String qid = info.getAppMetadata() == null
        ? "?" : new String(info.getAppMetadata());
    System.out.printf("[java-presign] endpoints=%d queryId=%s...%n",
        info.getEndpoints().size(), qid.substring(0, Math.min(8, qid.length())));
    long total = 0;
    for (FlightEndpoint ep : info.getEndpoints()) {
      if (ep.getTicket().getBytes().length != 0) {
        throw new AssertionError("presigned endpoint must carry EMPTY ticket: " + ep);
      }
      if (ep.getLocations().isEmpty()) {
        throw new AssertionError("presigned endpoint must carry location: " + ep);
      }
      String scheme = ep.getLocations().get(0).getUri().getScheme();
      if (!("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))) {
        throw new AssertionError("presigned endpoint location must be http(s): " + ep);
      }
      try (ArrowReader reader = session.openEndpoint(ep)) {
        while (reader.loadNextBatch()) {
          total += reader.getVectorSchemaRoot().getRowCount();
        }
      } catch (Exception e) {
        throw new IllegalStateException("openEndpoint failed at " + total + " rows", e);
      }
    }
    if (total != 1000) {
      throw new AssertionError("[java-presign] rows " + total + " != 1000");
    }
    System.out.printf("[java-presign] PASS (%.1fs)%n", elapsedSec(t0));
  }

  /**
   * D15 header 模式协商验证（网关保持默认 relay）：同一 SQL 携请求头
   * {@code x-fg-endpoint-mode: https} 执行（patched=poll）→ 注册落行 mode=HTTPS → 终态
   * endpoints 必为空票 + http(s) location（presigned URL）→ openEndpoint 计行 1000。
   * 与 presign 用例的差别：网关配置不动，模式翻转完全由客户端头驱动。
   */
  private static void modeHeader(FlightSqlSession session) throws Exception {
    long t0 = System.nanoTime();
    org.apache.arrow.flight.FlightCallHeaders headers =
        new org.apache.arrow.flight.FlightCallHeaders();
    headers.insert("x-fg-endpoint-mode", "https");
    org.apache.arrow.flight.HeaderCallOption modeHeader =
        new org.apache.arrow.flight.HeaderCallOption(headers);
    // SQL 与 legacy 案例区分开：fingerprint 幂等（user+sql）下同 SQL 复用在册行，
    // legacy 已以 RELAY 落行 → header 不再协商（mode 首注册落行，在册行优先）
    FlightInfo info =
        session.execute("SELECT id, id * 3 AS triple FROM range(1000)", modeHeader);
    long total = 0;
    for (FlightEndpoint ep : info.getEndpoints()) {
      if (ep.getTicket().getBytes().length != 0
          || ep.getLocations().isEmpty()
          || !("http".equalsIgnoreCase(ep.getLocations().get(0).getUri().getScheme())
              || "https".equalsIgnoreCase(ep.getLocations().get(0).getUri().getScheme()))) {
        throw new AssertionError("expected presigned endpoint via mode header, got: " + ep);
      }
      try (ArrowReader reader = session.openEndpoint(ep, modeHeader)) {
        while (reader.loadNextBatch()) {
          total += reader.getVectorSchemaRoot().getRowCount();
        }
      }
    }
    if (total != 1000) {
      throw new AssertionError("[java-mode-header] rows " + total + " != 1000");
    }
    System.out.printf("[java-mode-header] PASS endpoints=%d (%.1fs)%n",
        info.getEndpoints().size(), elapsedSec(t0));
  }

  /**
   * Java 原生 poll 链路（⑥ 矩阵原语线）：pollInfo 原语直驱（fg-p1 前 FlightSqlClient
   * 未封装 poll 语义时的唯一路径，保留作原语对照）：首 poll 注册+触发快返 → 轮询至终态
   * （descriptor unset）→ 逐 endpoint 取数。
   */
  private static void poll(FlightSqlSession session) throws InterruptedException {
    long t0 = System.nanoTime();
    byte[] command = com.google.protobuf.Any.pack(
            FlightSql.CommandStatementQuery.newBuilder()
                .setQuery("SELECT id, id * 2 AS dbl FROM range(1000)").build())
        .toByteArray();
    FlightDescriptor descriptor = FlightDescriptor.command(command);
    PollInfo info = null;
    for (int i = 0; i < 90; i++) {
      info = session.pollInfo(descriptor);
      if (info.getFlightDescriptor().isEmpty()) { // 终态：flight_descriptor unset
        System.out.printf("[java-poll] terminal after %d polls%n", i + 1);
        break;
      }
      Thread.sleep(1000);
    }
    if (info == null || !info.getFlightDescriptor().isEmpty()) {
      throw new AssertionError("not terminal in time");
    }
    streamAll(session, info.getFlightInfo(), 1000, "[java-poll]", t0);
  }

  private static double elapsedSec(long t0) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) / 1000.0;
  }

  private JavaClientE2E() {}
}
