package org.fg.frontend;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
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
            .authHandler(
                new org.apache.arrow.flight.auth.BasicServerAuthHandler(
                    new FgBasicAuthValidator(config)));
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
