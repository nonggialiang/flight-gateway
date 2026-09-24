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

  private final QueryOrchestrator orchestrator;
  private final EndpointsAssembler endpoints;
  private final ResultRelay relay;
  private final GatewayConfig config;
  private final ExecutorService relayExecutor;
  /** peerIdentity(token) → 会话（M1 简化：每用户一会话；cookie 双轨 M3） */
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

  /** 注册 + 触发 + 快返 STREAM 票（statement 与 prepared 垫片共用）。 */
  private FlightInfo registerAndQuickReturn(
      String sql, FlightProducer.CallContext context, FlightDescriptor descriptor)
      throws Exception {
    QueryOrchestrator.Registration reg =
        orchestrator.register(sessionRef(context), user(context), sql, negotiateMode());
    OperationRow row = reg.row();
    return flightInfo(
        schemaOf(row),
        descriptor,
        List.of(endpoints.streamTicketEndpoint(row)),
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
      if (!any.is(FlightSql.CommandStatementQuery.class)) {
        return super.pollFlightInfo(context, descriptor);
      }
      FlightSql.CommandStatementQuery command =
          any.unpack(FlightSql.CommandStatementQuery.class);
      String sql = command.getQuery();

      // ① 幂等注册 + 触发（首个 poll 触发；后续 poll 同 fingerprint 命中在途/终态行）
      QueryOrchestrator.Registration reg =
          orchestrator.register(sessionRef(context), user(context), sql, negotiateMode());
      OperationRow row = reg.row();

      // ②…长等待（fg.poll.max-wait 到点返回未完成，客户端续 poll）
      QueryOrchestrator.PollOutcome outcome =
          orchestrator.poll(
              row.queryId(), Duration.ofMillis(config.getDurationMs("fg.poll.max-wait")));

      if (!outcome.done()) {
        FlightInfo info =
            flightInfo(schemaOf(outcome.row()), descriptor, List.of(), -1L, -1L, false,
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

  // ------------------------------------------------------------- GetSchema（JDBC prepare 期需要）

  @Override
  public org.apache.arrow.flight.SchemaResult getSchema(
      FlightProducer.CallContext context, FlightDescriptor descriptor) {
    try {
      Any any = Any.parseFrom(descriptor.getCommand());
      String sql = null;
      if (any.is(FlightSql.CommandStatementQuery.class)) {
        sql = any.unpack(FlightSql.CommandStatementQuery.class).getQuery();
      } else if (any.is(FlightSql.CommandPreparedStatementQuery.class)) {
        sql = any.unpack(FlightSql.CommandPreparedStatementQuery.class)
            .getPreparedStatementHandle().toStringUtf8();
      }
      if (sql != null) {
        // 注册路径会经 AnalyzePlan 回填 schema（getSchema 不另起触发语义，幂等 fingerprint）
        return new org.apache.arrow.flight.SchemaResult(
            registerAndQuickReturn(sql, context, descriptor).getSchema());
      }
    } catch (Exception e) {
      throw invalid("Malformed descriptor: " + e.getMessage());
    }
    return super.getSchema(context, descriptor);
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
        .withFlightSqlServerReadOnly(true) // M1 仅查询；事务/更新不支持
        .send(command.getInfoList(), listener);
  }

  // ------------------- PreparedStatement（JDBC 兼容垫片；真 prepare + 参数绑定归 M3）：
  // JDBC 驱动的 executeQuery 走 CreatePreparedStatement action → CommandPreparedStatementQuery，
  // 而非裸 CommandStatementQuery。垫片语义：handle 即 SQL 明文（无绑定），execute 与
  // statement 路径完全一致。

  @Override
  public void createPreparedStatement(
      FlightSql.ActionCreatePreparedStatementRequest request,
      FlightProducer.CallContext context,
      FlightProducer.StreamListener<org.apache.arrow.flight.Result> listener) {
    try {
      // 注册+触发（execute 时幂等命中在途/终态行），等 AnalyzePlan 回填真实 dataset_schema
      // （驱动据其判定 StatementType；空 schema 会被当作 update）
      OperationRow row =
          orchestrator
              .register(sessionRef(context), user(context), request.getQuery(), negotiateMode())
              .row();
      byte[] schema = orchestrator.waitForSchema(row.queryId());
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
    try {
      FlightEndpoint endpoint = request.getFlightEndpoint();
      byte[] ticketBytes = endpoint.getTicket().getBytes();
      if (ticketBytes.length == 0) {
        // https：从 location 的对象路径重签 presign
        String uri =
            endpoint.getLocations().isEmpty()
                ? null
                : endpoint.getLocations().get(0).getUri().toString();
        if (uri == null) {
          listener.onError(invalid("No location"));
          return;
        }
        int ttl = (int) (config.getDurationMs("fg.result.presign.ttl") / 1000);
        String newUrl =
            org.fg.result.store.ObjectStores.fromConfig(config)
                .presignGet(objectKeyFromUrl(uri), ttl);
        listener.onNext(
            new FlightEndpoint(
                new Ticket(new byte[0]),
                java.time.Instant.now().plusSeconds(ttl),
                new org.apache.arrow.flight.Location(java.net.URI.create(newUrl))));
      } else {
        // relay：重铸同字段新票（新 issuedAt）
        String encoded = new String(ticketBytes, StandardCharsets.UTF_8);
        org.fg.result.ticket.RelayTicket old =
            endpoints.codec().decode(encoded, user(context));
        org.fg.result.ticket.RelayTicket fresh =
            new org.fg.result.ticket.RelayTicket(
                old.kind(),
                old.bucket(),
                old.resultKeyPrefix(),
                old.queryId(),
                old.user(),
                System.currentTimeMillis() / 1000,
                old.partIndex());
        listener.onNext(
            new FlightEndpoint(
                new Ticket(
                    endpoints.codec().encode(fresh).getBytes(StandardCharsets.UTF_8)),
                org.apache.arrow.flight.Location.reuseConnection()));
      }
      listener.onCompleted();
    } catch (Exception e) {
      listener.onError(invalid("Renew failed: " + e.getMessage()));
    }
  }

  private static String objectKeyFromUrl(String presignedUrl) {
    java.net.URI u = java.net.URI.create(presignedUrl);
    // path 形如 /{bucket}/{key}...（bucket 后为 key）
    String path = u.getPath();
    int secondSlash = path.indexOf('/', 1);
    return secondSlash > 0 ? path.substring(secondSlash + 1) : path.substring(1);
  }

  // ------------------------------------------------------------- helpers

  private String user(FlightProducer.CallContext context) {
    return sessions.computeIfAbsent(context.peerIdentity(), id -> context.peerIdentity());
  }

  private String sessionRef(FlightProducer.CallContext context) {
    // M1：会话引用=identity（token）；fingerprint 语义=同 token 同 SQL
    return context.peerIdentity();
  }

  private OperationRow.Mode negotiateMode() {
    // M1：mode 由服务端配置决定（D15 的 header/session option 协商接 M3 认证强化时引入）
    return OperationRow.Mode.parse(config.getString("fg.result.endpoint.mode"));
  }

  private Schema schemaOf(OperationRow row) {
    try {
      Schema schema = SchemaSerde.deserialize(row.schemaBytes());
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
    return new FlightInfo(
        schema,
        descriptor,
        endpoints0,
        records,
        bytes,
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
