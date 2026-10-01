package org.fg.frontend;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.ServerSessionMiddleware;
import org.apache.arrow.memory.BufferAllocator;
import org.fg.common.config.GatewayConfig;
import org.fg.common.service.Service;
import org.fg.orchestrator.QueryOrchestrator;
import org.fg.result.store.ObjectStoreService;
import org.fg.result.ticket.RelayTicketCodec;

/**
 * Flight 服务（design §4.1）：官方 FlightServer.Builder（v19 已内建 keepalive/idle/
 * maxConnectionAge，弃 Dremio fork）；relay 阻塞等待只在有界 relay-executor。
 */
public final class FgFlightService implements Service {

  private final GatewayConfig config;
  private final BufferAllocator allocator;
  private final QueryOrchestrator orchestrator;
  private final ObjectStoreService objects;
  private final java.util.concurrent.ExecutorService relayExecutor;
  private final io.micrometer.core.instrument.MeterRegistry metrics;

  private FlightServer server;
  private int boundPort;

  public FgFlightService(
      GatewayConfig config,
      BufferAllocator allocator,
      QueryOrchestrator orchestrator,
      ObjectStoreService objects,
      java.util.concurrent.ExecutorService relayExecutor,
      io.micrometer.core.instrument.MeterRegistry metrics) {
    this.config = config;
    this.allocator = allocator;
    this.orchestrator = orchestrator;
    this.objects = objects;
    this.relayExecutor = relayExecutor;
    this.metrics = metrics;
  }

  @Override
  public void start() throws java.io.IOException {
    RelayTicketCodec ticketCodec =
        new RelayTicketCodec(
            config.getString("fg.result.ticket.secret"),
            config.hasPath("fg.result.ticket.previous-secret")
                ? config.getString("fg.result.ticket.previous-secret")
                : null,
            Duration.ofMillis(config.getDurationMs("fg.result.ticket.ttl")));
    EndpointsAssembler endpoints = new EndpointsAssembler(config, objects, ticketCodec);
    ResultRelay relay =
        new ResultRelay(orchestrator, objects, ticketCodec, config, allocator, metrics);
    FgFlightProducer producer =
        new FgFlightProducer(orchestrator, endpoints, relay, config, relayExecutor, allocator);

    int port = config.getInt(GatewayConfig.FLIGHT_PORT);
    boolean tlsEnabled = config.getBoolean(GatewayConfig.FLIGHT_TLS_ENABLED);
    // D25：TLS 时 listen location 同步切 grpc+tls（bind host:port 不变，scheme 保持一致语义）
    Location listenLocation = tlsEnabled
        ? Location.forGrpcTls("0.0.0.0", port)
        : Location.forGrpcInsecure("0.0.0.0", port);
    FlightServer.Builder builder =
        FlightServer.builder(allocator, listenLocation, producer)
            // D15 header 协商：捕获 x-fg-endpoint-mode（https|relay），注册时落行
            .middleware(EndpointModeMiddleware.KEY, new EndpointModeMiddleware.Factory())
            // D27 scroll 判据：捕获 x-fg-result-set-type（scroll），注册时落行 + 强制 relay
            .middleware(ScrollModeMiddleware.KEY, new ScrollModeMiddleware.Factory())
            // D28 显式续传：捕获 x-fg-query-id（命中=续传 / 未命中=新建执行 / 缺失=指纹去重）
            .middleware(QueryIdMiddleware.KEY, new QueryIdMiddleware.Factory())
            // D27 页头：DoGet 时捕获 x-fg-page-offset/limit（STREAM 票页切片）
            .middleware(PagingMiddleware.KEY, new PagingMiddleware.Factory())
            // RPC 级调试日志（每个 RPC 一行 DEBUG：方法/peer/session/fg 头——调用顺序分析用）
            .middleware(RpcTraceMiddleware.KEY, new RpcTraceMiddleware.Factory())
            // D20 会话身份（fg-p2 严格模式）：服务端零铸造，身份一律客户端携带——
            // cookie（arrow_flight_session_id，供已持有者）或 x-fg-session-id 自报头
            //（主通道：FG JDBC 驱动 fg-p2 每连接自动生成；pyarrow/ADBC 经连接选项）
            .middleware(
                FgFlightProducer.SESSION_MIDDLEWARE_KEY,
                new ServerSessionMiddleware.Factory(() -> UUID.randomUUID().toString()))
            .middleware(SessionIdMiddleware.KEY, new SessionIdMiddleware.Factory())
            // auth2-only（Authorization: Basic/Bearer 头，design F1）：
            // arrow-java 的 Handshake RPC 只走 auth1 ServerAuthHandler（FlightService#handshake
            // → ServerAuthWrapper.wrapHandshake(authHandler,...)），双栈并存不可能（Dremio 亦
            // if/else 单选）；而 JDBC 驱动只说 auth2 → 统一 auth2。Bearer 优先/Basic 回退/签
            // 发 token 见 FgBearerTokenAuthenticator（DremioBearerTokenAuthenticator 范式）。
            .headerAuthenticator(new FgBearerTokenAuthenticator(config));
    if (tlsEnabled) {
      // D25：PEM 双文件 / KeyStore(PKCS12/JKS) 二选一，可选 mTLS 客户端验证
      //（材料缺失/混用在此快败，见 ServerTlsMaterial）
      ServerTlsMaterial tls = ServerTlsMaterial.load(config);
      builder.useTls(tls.certChain(), tls.key());
      if (tls.clientCaCert().isPresent()) {
        builder.useMTlsClientVerification(tls.clientCaCert().get());
      }
    }
    server = builder.build();
    server.start();
    this.boundPort = server.getPort();
  }

  public int boundPort() {
    return boundPort;
  }

  @Override
  public void close() {
    if (server != null) {
      try {
        server.close();
        server.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
