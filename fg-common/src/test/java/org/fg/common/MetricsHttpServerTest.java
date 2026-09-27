package org.fg.common;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.fg.common.config.GatewayConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** /metrics 暴露（D26）：Prometheus 文本格式 + JVM 基线 + fg.* 业务指标。 */
class MetricsHttpServerTest {

  private MetricsHttpServer server;
  private final HttpClient client = HttpClient.newHttpClient();

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.close();
    }
  }

  private MetricsHttpServer startOnRandomPort(PrometheusMeterRegistry registry) throws Exception {
    System.setProperty("fg.metrics.port", "0");
    try {
      GatewayConfig config = GatewayConfig.create();
      MetricsHttpServer s = new MetricsHttpServer(registry, config);
      s.start();
      return s;
    } finally {
      System.clearProperty("fg.metrics.port");
    }
  }

  @Test
  void exposesPrometheusTextWithJvmAndFgMeters() throws Exception {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    new io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics().bindTo(registry);
    registry.counter("fg.engine.launches").increment();

    server = startOnRandomPort(registry);
    HttpResponse<String> resp =
        client.send(
            HttpRequest.newBuilder(URI.create("http://localhost:" + server.boundPort() + "/metrics"))
                .GET().build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(resp.statusCode()).isEqualTo(200);
    assertThat(resp.body()).contains("fg_engine_launches_total 1");
    assertThat(resp.body()).contains("jvm_memory_used_bytes");
    assertThat(resp.headers().firstValue("Content-Type")).isPresent();
  }

  @Test
  void rootPathIs404() throws Exception {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    server = startOnRandomPort(registry);
    HttpResponse<String> resp =
        client.send(
            HttpRequest.newBuilder(URI.create("http://localhost:" + server.boundPort() + "/"))
                .GET().build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(resp.statusCode()).isEqualTo(404);
  }
}
