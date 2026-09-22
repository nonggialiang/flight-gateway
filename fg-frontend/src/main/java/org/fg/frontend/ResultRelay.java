package org.fg.frontend;

import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.fg.common.config.GatewayConfig;
import org.fg.orchestrator.QueryOrchestrator;
import org.fg.orchestrator.store.OperationRow;
import org.fg.result.manifest.ResultManifest;
import org.fg.result.store.ObjectStoreService;
import org.fg.result.ticket.RelayTicket;
import org.fg.result.ticket.RelayTicketCodec;
import org.fg.result.ticket.TicketKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DoGet relay 中继（design §4.7/D7）：验签 → 查行（在途则等待，同 poll 语义）→ 读 manifest →
 * STREAM=依序流全部 part / PART=只流该 part。
 *
 * <p>内存 O(1 batch)/流；背压骨架照 Dremio BackpressureStrategy（isReady 不满足则 wait，
 * 超时 fail）；长阻塞只在调用线程（relay-executor 有界池，由装配保证）。
 */
final class ResultRelay {

  private static final Logger logger = LoggerFactory.getLogger(ResultRelay.class);

  private final QueryOrchestrator orchestrator;
  private final ObjectStoreService objects;
  private final RelayTicketCodec ticketCodec;
  private final GatewayConfig config;
  private final BufferAllocator allocator;

  ResultRelay(
      QueryOrchestrator orchestrator,
      ObjectStoreService objects,
      RelayTicketCodec ticketCodec,
      GatewayConfig config,
      BufferAllocator allocator) {
    this.orchestrator = orchestrator;
    this.objects = objects;
    this.ticketCodec = ticketCodec;
    this.config = config;
    this.allocator = allocator;
  }

  void relay(String user, Ticket ticket, ServerStreamListener listener) {
    RelayTicket t;
    try {
      t = ticketCodec.decode(new String(ticket.getBytes(), java.nio.charset.StandardCharsets.UTF_8), user);
    } catch (RelayTicketCodec.ExpiredTicketException e) {
      listener.error(
          CallStatus.INVALID_ARGUMENT
              .withDescription("Ticket expired; re-poll for fresh endpoints")
              .toRuntimeException());
      return;
    } catch (Exception e) {
      listener.error(CallStatus.UNAUTHENTICATED.withDescription("Invalid ticket: " + e.getMessage()).toRuntimeException());
      return;
    }

    // 在途则等待，同 poll 语义
    QueryOrchestrator.PollOutcome outcome;
    try {
      outcome = orchestrator.poll(
          t.queryId(), Duration.ofMillis(config.getDurationMs("fg.poll.max-wait")));
    } catch (Exception e) {
      listener.error(CallStatus.INTERNAL.withDescription("Poll failed: " + e.getMessage()).toRuntimeException());
      return;
    }
    if (!outcome.done()) {
      listener.error(
          CallStatus.UNAVAILABLE.withDescription("Result not ready yet; re-poll").toRuntimeException());
      return;
    }
    OperationRow row = outcome.row();
    switch (row.status()) {
      case COMPLETED -> { /* fall through to stream */ }
      case CANCELLED -> {
        listener.error(CallStatus.CANCELLED.withDescription("Query was cancelled").toRuntimeException());
        return;
      }
      default -> {
        listener.error(
            CallStatus.INTERNAL.withDescription("Query failed: " + (row.error() == null ? row.status() : row.error())).toRuntimeException());
        return;
      }
    }

    ResultManifest manifest = outcome.manifest();
    if (manifest == null) {
      listener.error(CallStatus.INTERNAL.withDescription("Manifest missing for completed query").toRuntimeException());
      return;
    }

    List<ResultManifest.Part> parts = manifest.parts();
    if (t.kind() == TicketKind.PART) {
      parts =
          parts.stream()
              .filter(p -> p.index() == t.partIndex())
              .toList();
      if (parts.isEmpty()) {
        listener.error(CallStatus.NOT_FOUND.withDescription("Unknown partIndex: " + t.partIndex()).toRuntimeException());
        return;
      }
    }

    long readinessTimeoutMs = config.getDurationMs("fg.relay.client.readiness.timeout");
    BufferAllocator child = allocator.newChildAllocator("fg-relay-" + t.queryId(), 0, Long.MAX_VALUE);
    VectorSchemaRoot out = null;
    try {
      // 单一输出 root（putNext 序列化的是 start() 注册的 root 实例）；
      // 各 part 经 VectorUnloader/VectorLoader 装载进该 root
      org.apache.arrow.vector.VectorLoader loader = null;
      long rows = 0;
      for (ResultManifest.Part part : parts) {
        try (InputStream in = objects.getObject(ObjectStoreService.objectName(part.uri()));
            ArrowStreamReader reader = new ArrowStreamReader(in, child)) {
          VectorSchemaRoot src = reader.getVectorSchemaRoot();
          if (out == null) {
            out = VectorSchemaRoot.create(src.getSchema(), child);
            listener.start(out);
            loader = new org.apache.arrow.vector.VectorLoader(out);
          }
          while (reader.loadNextBatch()) {
            org.apache.arrow.vector.ipc.message.ArrowRecordBatch batch =
                new org.apache.arrow.vector.VectorUnloader(src).getRecordBatch();
            try {
              loader.load(batch);
            } finally {
              batch.close();
            }
            putNextWhenClientReady(listener, readinessTimeoutMs, t.queryId());
            rows += out.getRowCount();
          }
        }
      }
      logger.info("Relay complete: {} parts={} rows={}", t.queryId(), parts.size(), rows);
      listener.completed();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      listener.error(CallStatus.INTERNAL.withDescription("Relay interrupted").toRuntimeException());
    } catch (Exception e) {
      logger.warn("Relay failed for {}: {}", t.queryId(), e.toString());
      listener.error(CallStatus.INTERNAL.withDescription("Relay failed: " + e.getMessage()).toRuntimeException());
    } finally {
      if (out != null) {
        out.close();
      }
      child.close();
    }
  }

  /**
   * 背压等待（Dremio BackpressureStrategy 骨架）：isReady 不满足则 wait，
   * 被 onReady 唤醒或超时 fail；isCancelled 直接终止。
   */
  private void putNextWhenClientReady(
      ServerStreamListener listener, long timeoutMs, String queryId) throws InterruptedException {
    Object monitor = new Object();
    listener.setOnReadyHandler(monitor::notifyAll);
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    synchronized (monitor) {
      while (!listener.isReady()) {
        if (listener.isCancelled()) {
          throw new InterruptedException("Client cancelled stream: " + queryId);
        }
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          throw new IllegalStateException(
              "Client not ready for " + timeoutMs + "ms (backpressure timeout): " + queryId);
        }
        monitor.wait(Math.min(remaining, 1000));
      }
    }
    listener.putNext();
  }
}
