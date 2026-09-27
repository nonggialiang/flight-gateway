package org.fg.common;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import org.fg.common.config.GatewayConfig;
import org.fg.common.service.Service;

/**
 * Prometheus 抓取端点（D26）：{@code GET /metrics} → {@code PrometheusMeterRegistry#scrape()}
 * 文本格式（含 JVM 基线 + fg.* 业务指标）。只读、信任域内暴露（与 Flight 端口同级），
 * 无鉴权；{@code fg.metrics.port=0} 时随机端口（{@link #boundPort()} 可查，测试用）。
 *
 * <p>注册为 {@link Service} 随 SingletonRegistry 生命周期启停（reverse-close 先于
 * FlightServer 关闭，抓取窗口覆盖在活期）。
 */
public final class MetricsHttpServer implements Service {

  private final PrometheusMeterRegistry registry;
  private final int port;
  private HttpServer server;

  public MetricsHttpServer(PrometheusMeterRegistry registry, GatewayConfig config) {
    this.registry = registry;
    this.port = config.getInt("fg.metrics.port");
  }

  @Override
  public void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext("/metrics", exchange -> {
      byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
    });
    server.createContext("/", exchange -> {
      byte[] body = "fg metrics: GET /metrics\n".getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(404, body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
    });
    server.setExecutor(Executors.newFixedThreadPool(1, r -> {
      Thread t = new Thread(r, "fg-metrics-http");
      t.setDaemon(true);
      return t;
    }));
    server.start();
  }

  /** 实际绑定端口（port=0 随机分配时用）。 */
  public int boundPort() {
    return server == null ? port : server.getAddress().getPort();
  }

  @Override
  public void close() {
    if (server != null) {
      server.stop(0);
    }
  }
}
