package org.fg.frontend;

import com.google.protobuf.Any;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CancelFlightInfoRequest;
import org.apache.arrow.flight.CancelStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.PollInfo;
import org.apache.arrow.flight.RenewFlightEndpointRequest;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.NoOpFlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Schema;
import org.fg.common.config.GatewayConfig;
import org.fg.common.sql.StatementClassifier;
import org.fg.orchestrator.CommandSchemas;
import org.fg.orchestrator.QueryOrchestrator;
import org.fg.orchestrator.SchemaSerde;
import org.fg.orchestrator.store.OperationRow;

/**
 * Flight SQL producer（design §4.1/F1/F2）。
 *
 * <p>主链路：PollFlightInfo（首=注册+触发+快返，后续=长等待至终态一次性交付 endpoints）；
 * GetFlightInfo 旧客户端兼容路径（同语义快返 STREAM 票 endpoint）；CancelFlightInfo 标准
 * 取消；RenewFlightEndpoint 续期；DoGet=relay 中继。元数据目录族（M2）暂走 NoOp 默认。
 */
final class FgFlightProducer extends NoOpFlightSqlProducer {

  /** cookie 会话 middleware 注册键（ServerSessionMiddleware，per-connection 主通道）。 */
  static final org.apache.arrow.flight.FlightServerMiddleware.Key<
          org.apache.arrow.flight.ServerSessionMiddleware>
      SESSION_MIDDLEWARE_KEY = org.apache.arrow.flight.FlightServerMiddleware.Key.of("fg-session");

  private final QueryOrchestrator orchestrator;
  private final EndpointsAssembler endpoints;
  private final ResultRelay relay;
  private final GatewayConfig config;
  private final ExecutorService relayExecutor;
  /** peerIdentity(user) → user 直通缓存（identity 即用户名；会话语义见 sessionRef） */
  private final Map<String, String> sessions = new ConcurrentHashMap<>();

  FgFlightProducer(
      QueryOrchestrator orchestrator,
      EndpointsAssembler endpoints,
      ResultRelay relay,
      GatewayConfig config,
      ExecutorService relayExecutor) {
    this.orchestrator = orchestrator;
    this.endpoints = endpoints;
    this.relay = relay;
    this.config = config;
    this.relayExecutor = relayExecutor;
  }

  // ------------------------------------------------------------- GetFlightInfo（旧客户端）

  @Override
  public FlightInfo getFlightInfo(
      FlightProducer.CallContext context, FlightDescriptor descriptor) {
    try {
      Any any = Any.parseFrom(descriptor.getCommand());
      if (any.is(FlightSql.CommandStatementQuery.class)) {
        FlightSql.CommandStatementQuery command =
            any.unpack(FlightSql.CommandStatementQuery.class);
        return legacyGetFlightInfo(command, context, descriptor);
      }
    } catch (Exception e) {
      throw invalid("Malformed descriptor: " + e.getMessage());
    }
    return super.getFlightInfo(context, descriptor);
  }

  /** H1：快返（提交时即签发 STREAM 票，DoGet 挂等待）。 */
  private FlightInfo legacyGetFlightInfo(
      FlightSql.CommandStatementQuery command,
      FlightProducer.CallContext context,
      FlightDescriptor descriptor) {
    try {
      return registerAndQuickReturn(command.getQuery(), context, descriptor);
    } catch (Exception e) {
      throw internal("Register failed: " + e.getMessage());
    }
  }

  /**
   * 注册 + 触发 + 快返票（statement 与 prepared 垫片共用）。票可即刻给（DoGet 反正要等）：
   * QUERY→STREAM 票、COMMAND→COMMAND 定位票（D18，DoGet 挂等待后流行内结果）。宣告 schema
   * 随行出生（register 同步 AnalyzePlan/静态宣告，失败即注册失败）——严格客户端（ADBC）逐
   * endpoint 校验宣告 schema 与流 schema 一致，空 schema 会直接拒收。
   */
  private FlightInfo registerAndQuickReturn(
      String sql, FlightProducer.CallContext context, FlightDescriptor descriptor)
      throws Exception {
    QueryOrchestrator.Registration reg =
        orchestrator.register(sessionRef(context), user(context), sql, negotiateMode(context));
    OperationRow row = reg.row();
    return flightInfo(
        schemaOf(row.schemaBytes()),
        descriptor,
        List.of(
            row.kind() == OperationRow.Kind.COMMAND
                ? endpoints.commandEndpoint(row)
                : endpoints.streamEndpoint(row)),
        -1L,
        -1L,
        false,
        row.queryId());
  }

  // ------------------------------------------------------------- PollFlightInfo（主链路）

  @Override
  public PollInfo pollFlightInfo(
      FlightProducer.CallContext context, FlightDescriptor descriptor) {
    try {
      Any any = Any.parseFrom(descriptor.getCommand());
      // CommandPreparedStatementQuery 同 getFlightInfoPreparedStatement 垫片：handle 即 SQL 明文。
      // patched JDBC（PreparedStatement.execute 走 PollFlightInfo）依赖此分支，否则 UNIMPLEMENTED
      // 触发客户端回退 GetFlightInfo → 永远到不了 presign 终态。
      String sql = null;
      if (any.is(FlightSql.CommandStatementQuery.class)) {
        sql = any.unpack(FlightSql.CommandStatementQuery.class).getQuery();
      } else if (any.is(FlightSql.CommandPreparedStatementQuery.class)) {
        sql = any.unpack(FlightSql.CommandPreparedStatementQuery.class)
            .getPreparedStatementHandle().toStringUtf8();
      }
      if (sql == null) {
        return super.pollFlightInfo(context, descriptor);
      }

      // ① 幂等注册 + 触发（首个 poll 触发；后续 poll 同 fingerprint 命中在途/终态行）
      QueryOrchestrator.Registration reg =
          orchestrator.register(sessionRef(context), user(context), sql, negotiateMode(context));
      OperationRow row = reg.row();

      // ②…长等待（fg.poll.max-wait 到点返回未完成，客户端续 poll）
      QueryOrchestrator.PollOutcome outcome =
          orchestrator.poll(
              row.queryId(), Duration.ofMillis(config.getDurationMs("fg.poll.max-wait")));

      if (!outcome.done()) {
        // not-done PollOutcome.row 按契约为 null；schema 取 register 行（行带 schema 出生）
        FlightInfo info =
            flightInfo(schemaOf(row), descriptor, List.of(), -1L, -1L, false,
                row.queryId());
        return new PollInfo(info, descriptor, null, null); // descriptor set → 继续轮询
      }

      OperationRow terminal = outcome.row();
      switch (terminal.status()) {
        case FAILED:
          throw internal(
              "Query failed: " + (terminal.error() == null ? "unknown" : terminal.error()));
        case CANCELLED:
          throw CallStatus.CANCELLED.withDescription("Query was cancelled").toRuntimeException();
        case COMPLETED:
        default:
      }

      // 终态：一次性返回全部 endpoints（flight_descriptor unset → 终结；ordered 按行回填）
      if (terminal.kind() == OperationRow.Kind.COMMAND) {
        // D18：命令终态——单 COMMAND 定位票、无 manifest 计量（-1/-1，ordered 无意义）
        FlightInfo cmdInfo =
            flightInfo(
                schemaOf(terminal),
                descriptor,
                List.of(endpoints.commandEndpoint(terminal)),
                -1L,
                -1L,
                false,
                terminal.queryId());
        return new PollInfo(cmdInfo, null, null, null);
      }
      List<FlightEndpoint> eps = endpoints.endpoints(terminal, outcome.manifest());
      FlightInfo info =
          flightInfo(
              schemaOf(terminal),
              descriptor,
              eps,
              outcome.manifest().rowCount(),
              outcome.manifest().bytes(),
              terminal.ordered(),
              terminal.queryId());
      return new PollInfo(info, null, null, null);
    } catch (FlightRuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw internal("Poll failed: " + e.getMessage());
    }
  }

  // ------------------------------------------------------------- GetSchema（plan-only，不触发执行）

  @Override
  public org.apache.arrow.flight.SchemaResult getSchema(
      FlightProducer.CallContext context, FlightDescriptor descriptor) {
    String sql;
    try {
      Any any = Any.parseFrom(descriptor.getCommand());
      if (any.is(FlightSql.CommandStatementQuery.class)) {
        sql = any.unpack(FlightSql.CommandStatementQuery.class).getQuery();
      } else if (any.is(FlightSql.CommandPreparedStatementQuery.class)) {
        sql = any.unpack(FlightSql.CommandPreparedStatementQuery.class)
            .getPreparedStatementHandle().toStringUtf8();
      } else {
        return super.getSchema(context, descriptor);
      }
    } catch (Exception e) {
      throw invalid("Malformed descriptor: " + e.getMessage());
    }
    // Flight SQL 语义 GetSchema = plan-only：AnalyzePlan 直取 schema，不注册、不触发执行。
    // D17：非 analyzable 命令（SET/RESET/USE/DML/DDL）不可分析——静态宣告 schema，不碰引擎
    try {
      byte[] schema = analyzeOrStaticSchema(context, sql);
      return new org.apache.arrow.flight.SchemaResult(
          schema == null ? new Schema(List.of()) : SchemaSerde.deserialize(schema));
    } catch (FlightRuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw internal("AnalyzePlan failed: " + e.getMessage());
    }
  }

  // ------------------------------------------------------------- DoGet（relay）

  @Override
  public void getStream(
      FlightProducer.CallContext context, Ticket ticket,
      FlightProducer.ServerStreamListener listener) {
    // FlightSql 命令票（Any 打包，如 SqlInfo）走 producer 类型化路由；HMAC 信封票走 relay
    try {
      Any any = Any.parseFrom(ticket.getBytes());
      if (any.getTypeUrl().startsWith("type.googleapis.com/arrow.flight.protocol.sql.")) {
        super.getStream(context, ticket, listener);
        return;
      }
    } catch (Exception e) {
      // 非 protobuf 票 → HMAC 信封
    }
    // relay 阻塞等待只在有界 relay-executor（设计 §4.1）
    relayExecutor.execute(() -> relay.relay(context.peerIdentity(), ticket, listener));
  }

  // ------------------- SqlInfo（JDBC DatabaseMetaData 最小面，设计 7.4；SqlInfoBuilder 照 Dremio 范式）

  @Override
  public FlightInfo getFlightInfoSqlInfo(
      FlightSql.CommandGetSqlInfo command,
      FlightProducer.CallContext context,
      FlightDescriptor descriptor) {
    return new FlightInfo(
        org.apache.arrow.flight.sql.FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA,
        descriptor,
        List.of(new FlightEndpoint(new Ticket(Any.pack(command).toByteArray()))),
        -1,
        -1);
  }

  @Override
  public void getStreamSqlInfo(
      FlightSql.CommandGetSqlInfo command,
      FlightProducer.CallContext context,
      FlightProducer.ServerStreamListener listener) {
    new org.apache.arrow.flight.sql.SqlInfoBuilder()
        .withFlightSqlServerName("Flight Gateway")
        .withFlightSqlServerVersion("0.1.0")
        // D17：非 SELECT 语句支持上线——服务端非只读，DDL 三能力全开（catalog 归引擎）
        .withFlightSqlServerReadOnly(false)
        .withSqlDdlCatalog(true)
        .withSqlDdlSchema(true)
        .withSqlDdlTable(true)
        .send(command.getInfoList(), listener);
  }

  // ------------------- PreparedStatement（JDBC 兼容垫片；真 prepare + 参数绑定归 M3）：
  // JDBC 驱动的 executeQuery 走 CreatePreparedStatement action → CommandPreparedStatementQuery，
  // 而非裸 CommandStatementQuery。垫片语义：handle 即 SQL 明文（无绑定），execute 与
  // statement 路径完全一致。prepare 本身 plan-only（AnalyzePlan 直取 dataset_schema，不注册、
  // 不触发），执行延迟到首个 GetFlightInfo/PollFlightInfo（lazy trigger）。

  @Override
  public void createPreparedStatement(
      FlightSql.ActionCreatePreparedStatementRequest request,
      FlightProducer.CallContext context,
      FlightProducer.StreamListener<org.apache.arrow.flight.Result> listener) {
    try {
      // plan-only：dataset_schema 直接来自 AnalyzePlan（驱动据其判定 StatementType；
      // 空 schema 会被当作 update）。AnalyzePlan 失败（如语法错误）即 prepare 失败。
      // D17：非 analyzable 命令用静态宣告 schema（驱动据此走 executeUpdate 分支）
      byte[] schema = analyzeOrStaticSchema(context, request.getQuery());
      FlightSql.ActionCreatePreparedStatementResult.Builder result =
          FlightSql.ActionCreatePreparedStatementResult.newBuilder()
              .setPreparedStatementHandle(
                  com.google.protobuf.ByteString.copyFromUtf8(request.getQuery()));
      if (schema != null) {
        result.setDatasetSchema(com.google.protobuf.ByteString.copyFrom(schema));
      }
      // Result body = Any{ActionCreatePreparedStatementResult}（驱动端 Any.unpack 约定）
      listener.onNext(
          new org.apache.arrow.flight.Result(Any.pack(result.build()).toByteArray()));
      listener.onCompleted();
    } catch (Exception e) {
      listener.onError(internal("Prepare failed: " + e.getMessage()));
    }
  }

  @Override
  public void closePreparedStatement(
      FlightSql.ActionClosePreparedStatementRequest request,
      FlightProducer.CallContext context,
      FlightProducer.StreamListener<org.apache.arrow.flight.Result> listener) {
    listener.onCompleted(); // 垫片无服务端状态
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(
      FlightSql.CommandPreparedStatementQuery command,
      FlightProducer.CallContext context,
      FlightDescriptor descriptor) {
    try {
      return registerAndQuickReturn(
          command.getPreparedStatementHandle().toStringUtf8(), context, descriptor);
    } catch (Exception e) {
      throw internal("Register failed: " + e.getMessage());
    }
  }

  /** JDBC 驱动 executeQuery 前必 DoPut 参数批（零参数亦然）——垫片消费并 ack，不落绑定。 */
  @Override
  public Runnable acceptPutPreparedStatementQuery(
      FlightSql.CommandPreparedStatementQuery command,
      FlightProducer.CallContext context,
      org.apache.arrow.flight.FlightStream flightStream,
      FlightProducer.StreamListener<org.apache.arrow.flight.PutResult> ackStream) {
    return () -> {
      ackStream.onNext(org.apache.arrow.flight.PutResult.empty()); // 驱动需读到至少一条 ack
      ackStream.onCompleted();
    };
  }

  @Override
  public Runnable acceptPutPreparedStatementUpdate(
      FlightSql.CommandPreparedStatementUpdate command,
      FlightProducer.CallContext context,
      org.apache.arrow.flight.FlightStream flightStream,
      FlightProducer.StreamListener<org.apache.arrow.flight.PutResult> ackStream) {
    return () -> {
      ackStream.onNext(org.apache.arrow.flight.PutResult.empty()); // 驱动需读到至少一条 ack
      ackStream.onCompleted();
    };
  }

  // ------------------------------------------------------------- 取消 / 续期

  /**
   * CancelFlightInfo / RenewFlightEndpoint 是 Flight SQL action（arrow-java 的
   * FlightSqlProducer 默认不识别，需在此分发；兼容两种类型名拼写）。
   */
  @Override
  public void doAction(
      FlightProducer.CallContext context,
      org.apache.arrow.flight.Action action,
      FlightProducer.StreamListener<org.apache.arrow.flight.Result> listener) {
    switch (action.getType()) {
      case "CancelFlightInfo", "arrow.flight.protocol.sql.CancelFlightInfo" -> {
        try {
          CancelFlightInfoRequest request = CancelFlightInfoRequest.deserialize(java.nio.ByteBuffer.wrap(action.getBody()));
          cancelFlightInfo(
              request,
              context,
              new FlightProducer.StreamListener<CancelStatus>() {
                @Override
                public void onNext(CancelStatus status) {
                  // Result body 必须是规范消息 CancelFlightInfoResult{status=1}；
                  // 此前手写裸 varint（0x01）被规范客户端（arrow-java FlightSqlClient）
                  // 以 invalid tag 拒绝——python 用例的 raw[0] 兜底掩盖了这一偏差
                  listener.onNext(
                      new org.apache.arrow.flight.Result(
                          org.apache.arrow.flight.impl.Flight.CancelFlightInfoResult.newBuilder()
                              .setStatus(
                                  org.apache.arrow.flight.impl.Flight.CancelStatus.valueOf(
                                      "CANCEL_STATUS_" + status.name()))
                              .build()
                              .toByteArray()));
                }

                @Override
                public void onError(Throwable t) {
                  listener.onError(t);
                }

                @Override
                public void onCompleted() {}
              });
          listener.onCompleted();
        } catch (Exception e) {
          listener.onError(invalid("Cancel failed: " + e.getMessage()));
        }
      }
      case "RenewFlightEndpoint", "arrow.flight.protocol.sql.RenewFlightEndpoint" -> {
        try {
          RenewFlightEndpointRequest request =
              RenewFlightEndpointRequest.deserialize(java.nio.ByteBuffer.wrap(action.getBody()));
          renewFlightEndpoint(
              request,
              context,
              new FlightProducer.StreamListener<FlightEndpoint>() {
                @Override
                public void onNext(FlightEndpoint endpoint) {
                  try {
                    listener.onNext(
                        new org.apache.arrow.flight.Result(flightEndpointBytes(endpoint)));
                  } catch (Exception e) {
                    listener.onError(invalid("Renew serialize failed"));
                  }
                }

                @Override
                public void onError(Throwable t) {
                  listener.onError(t);
                }

                @Override
                public void onCompleted() {}
              });
          listener.onCompleted();
        } catch (Exception e) {
          listener.onError(invalid("Renew failed: " + e.getMessage()));
        }
      }
      default -> super.doAction(context, action, listener);
    }
  }

  @Override
  public void cancelFlightInfo(
      CancelFlightInfoRequest request,
      FlightProducer.CallContext context,
      FlightProducer.StreamListener<CancelStatus> listener) {
    String queryId = null;
    try {
      queryId = new String(request.getInfo().getAppMetadata(), StandardCharsets.UTF_8);
    } catch (Exception ignored) {
      // fall through
    }
    if (queryId == null || queryId.isBlank()) {
      listener.onError(invalid("No queryId in FlightInfo"));
      return;
    }
    QueryOrchestrator.CancelOutcome outcome = orchestrator.cancel(queryId);
    switch (outcome) {
      case CANCELLED -> listener.onNext(CancelStatus.CANCELLED);
      case NOT_CANCELLABLE -> listener.onNext(CancelStatus.NOT_CANCELLABLE);
      default -> listener.onNext(CancelStatus.UNSPECIFIED);
    }
    listener.onCompleted();
  }

  @Override
  public void renewFlightEndpoint(
      RenewFlightEndpointRequest request,
      FlightProducer.CallContext context,
      FlightProducer.StreamListener<FlightEndpoint> listener) {
    // 票形分派：空票 = presign 形态（按 location 重签），HMAC 信封 = relay 形态（重铸 issuedAt）。
    // endpoint/ticket 构造统一收敛在 EndpointsAssembler——续期与首发共用同一套构造器。
    FlightEndpoint expired = request.getFlightEndpoint();
    try {
      FlightEndpoint renewed =
          expired.getTicket().getBytes().length == 0
              ? endpoints.renewedPresignedEndpoint(expired)
              : endpoints.renewedRelayEndpoint(expired, user(context));
      listener.onNext(renewed);
      listener.onCompleted();
    } catch (Exception e) {
      listener.onError(invalid("Renew failed: " + e.getMessage()));
    }
  }

  // ------------------------------------------------------------- helpers

  /**
   * plan-only schema 解析（GetSchema / CreatePreparedStatement 共用）：analyzable 语句
   * （QUERY/SHOW/DESCRIBE/EXPLAIN）走引擎 AnalyzePlan；其余（SET/RESET/USE/DML/DDL）不可
   * 分析（D17）——CommandSchemas 静态宣告，不碰引擎。
   */
  private byte[] analyzeOrStaticSchema(FlightProducer.CallContext context, String sql)
      throws Exception {
    StatementClassifier.Kind kind = StatementClassifier.classify(sql);
    if (!StatementClassifier.analyzable(kind)) {
      Schema declared = CommandSchemas.staticSchema(kind);
      return declared == null ? null : SchemaSerde.serialize(declared);
    }
    return orchestrator.analyzeSchema(sessionRef(context), user(context), sql);
  }

  private String user(FlightProducer.CallContext context) {
    return sessions.computeIfAbsent(context.peerIdentity(), id -> context.peerIdentity());
  }

  private String sessionRef(FlightProducer.CallContext context) {
    // per-connection 会话（原 M3 "cookie 双轨" 提前到 M1.5）：一个客户端连接 =
    // 一个 fg 会话 = 一个 Connect session = 引擎侧一个 SparkSession。三级解析：
    //   ① cookie 会话（JDBC/ADBC 等带 cookie jar 的客户端；arrow_flight_session_id
    //     由 ServerSessionMiddleware 绑定，SetSessionOptions 等类型化入口触发）；
    //   ② Authorization 头派生（无 cookie 客户端兜底：Bearer token 每连接一次握手、
    //     连接内稳定；Basic 直发退化场景同值稳定）——sha256 前缀，凭证材料不落库；
    //   ③ peerIdentity 兜底（auth2-only 下必带 Authorization 头，理论不可达）。
    // fingerprint 幂等语义随之变为"同连接同 SQL 不重复执行"（跨连接本就不该共享）。
    org.apache.arrow.flight.ServerSessionMiddleware session =
        context.getMiddleware(SESSION_MIDDLEWARE_KEY);
    if (session != null && session.hasSession()) {
      return session.getSession().id;
    }
    AuthHeaderMiddleware auth = context.getMiddleware(AuthHeaderMiddleware.KEY);
    if (auth != null && auth.authorization() != null && !auth.authorization().isBlank()) {
      return "auth-" + QueryOrchestrator.sha256(auth.authorization()).substring(0, 16);
    }
    return context.peerIdentity();
  }

  /**
   * D15 endpoint 模式协商（注册时生效、随即落行，后续同指纹 RPC 按行构造不再受头影响）：
   * 客户端请求头 {@code x-fg-endpoint-mode}（{@code https|relay}，大小写不敏感，经
   * {@link EndpointModeMiddleware} 捕获）优先；未携带/非法值回退服务端配置
   * {@code fg.result.endpoint.mode}（默认 relay）。session option 通道归 M3。
   */
  private OperationRow.Mode negotiateMode(FlightProducer.CallContext context) {
    EndpointModeMiddleware negotiated = context.getMiddleware(EndpointModeMiddleware.KEY);
    if (negotiated != null && negotiated.requestedMode() != null) {
      String requested = negotiated.requestedMode().trim();
      for (OperationRow.Mode mode : OperationRow.Mode.values()) {
        if (mode.name().equalsIgnoreCase(requested)) {
          return mode;
        }
      }
      // 非法值不阻断：回退配置默认
    }
    return OperationRow.Mode.parse(config.getString("fg.result.endpoint.mode"));
  }

  private Schema schemaOf(OperationRow row) {
    return schemaOf(row.schemaBytes());
  }

  private Schema schemaOf(byte[] schemaBytes) {
    try {
      Schema schema = SchemaSerde.deserialize(schemaBytes);
      if (schema != null) {
        return schema;
      }
    } catch (Exception ignored) {
      // fall through
    }
    return new Schema(List.of());
  }

  private static FlightInfo flightInfo(
      Schema schema,
      FlightDescriptor descriptor,
      List<FlightEndpoint> endpoints0,
      long records,
      long bytes,
      boolean ordered,
      String queryId) {
    // 注意 arrow-java FlightInfo 构造器参数序是 (bytes, records)——bytes 在前
    // （曾按 (records, bytes) 传入导致两个数上线起一直互换：total_records 报字节数）
    return new FlightInfo(
        schema,
        descriptor,
        endpoints0,
        bytes,
        records,
        ordered,
        new IpcOption(),
        queryId.getBytes(StandardCharsets.UTF_8));
  }

  private static byte[] flightEndpointBytes(FlightEndpoint endpoint) throws Exception {
    java.io.ByteArrayOutputStream b = new java.io.ByteArrayOutputStream();
    b.write(endpoint.serialize().array());
    return b.toByteArray();
  }

  private static FlightRuntimeException invalid(String message) {
    return CallStatus.INVALID_ARGUMENT.withDescription(message).toRuntimeException();
  }

  private static FlightRuntimeException internal(String message) {
    return CallStatus.INTERNAL.withDescription(message).toRuntimeException();
  }
}
