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

  private FlightServer server;
  private int boundPort;

  public FgFlightService(
      GatewayConfig config,
      BufferAllocator allocator,
      QueryOrchestrator orchestrator,
      ObjectStoreService objects,
      java.util.concurrent.ExecutorService relayExecutor) {
    this.config = config;
    this.allocator = allocator;
    this.orchestrator = orchestrator;
    this.objects = objects;
    this.relayExecutor = relayExecutor;
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
        new ResultRelay(orchestrator, objects, ticketCodec, config, allocator);
    FgFlightProducer producer =
        new FgFlightProducer(orchestrator, endpoints, relay, config, relayExecutor);

    int port = config.getInt(GatewayConfig.FLIGHT_PORT);
    Location listenLocation = Location.forGrpcInsecure("0.0.0.0", port);
    FlightServer.Builder builder =
        FlightServer.builder(allocator, listenLocation, producer)
            // D15 header 协商：捕获 x-fg-endpoint-mode（https|relay），注册时落行
            .middleware(EndpointModeMiddleware.KEY, new EndpointModeMiddleware.Factory())
            // per-connection 会话主通道（原 M3 "cookie 双轨" 提前）：arrow 官方 cookie 会话
            // （arrow_flight_session_id），JDBC/ADBC 客户端内建 cookie jar 即粘住。会话在
            // SetSessionOptions 等类型化入口经 getSession() 惰性绑定；普通 RPC 不主动建会话
            // （无 cookie jar 的客户端每个 RPC 都会新建会话，反致 sessionRef 漂移）。
            .middleware(
                FgFlightProducer.SESSION_MIDDLEWARE_KEY,
                new ServerSessionMiddleware.Factory(() -> UUID.randomUUID().toString()))
            // per-connection 会话兜底通道：捕获 Authorization 头（Bearer token 每连接一次
            // 握手、连接内稳定），无 cookie 客户端（pyarrow 等）据此派生按连接 sessionRef
            .middleware(AuthHeaderMiddleware.KEY, new AuthHeaderMiddleware.Factory())
            // auth2-only（Authorization: Basic/Bearer 头，design F1）：
            // arrow-java 的 Handshake RPC 只走 auth1 ServerAuthHandler（FlightService#handshake
            // → ServerAuthWrapper.wrapHandshake(authHandler,...)），双栈并存不可能（Dremio 亦
            // if/else 单选）；而 JDBC 驱动只说 auth2 → 统一 auth2。Bearer 优先/Basic 回退/签
            // 发 token 见 FgBearerTokenAuthenticator（DremioBearerTokenAuthenticator 范式）。
            .headerAuthenticator(new FgBearerTokenAuthenticator(config));
    if (config.getBoolean(GatewayConfig.FLIGHT_TLS_ENABLED)) {
      // M3：KeyStore→PEM（照 Dremio SSLConfigurator 范式）；M1 开发态默认关闭
      throw new IllegalStateException(
          "TLS requires keystore wiring (M3); set fg.flight.tls.enabled=false for dev");
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
