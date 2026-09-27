package org.fg.frontend;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
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
 * DoGet relay 中继（design §4.7/D7/D18）：验签 → 查行（在途则等待，同 poll 语义）→
 * STREAM=依序流全部 part / PART=只流该 part / COMMAND=流行内 command_result。
 *
 * <p>内存 O(1 batch)/流；背压骨架照 Dremio BackpressureStrategy（isReady 不满足则 wait，
 * 超时 fail）；长阻塞只在调用线程（relay-executor 有界池，由装配保证）。三种票共享同一条
 * {@link #pumpReader} 装载/背压管线（单一输出 root + VectorLoader 逐批装载 + putNext）。
 */
final class ResultRelay {

  private static final Logger logger = LoggerFactory.getLogger(ResultRelay.class);

  private final QueryOrchestrator orchestrator;
  private final ObjectStoreService objects;
  private final RelayTicketCodec ticketCodec;
  private final GatewayConfig config;
  private final BufferAllocator allocator;
  private final io.micrometer.core.instrument.MeterRegistry metrics;

  ResultRelay(
      QueryOrchestrator orchestrator,
      ObjectStoreService objects,
      RelayTicketCodec ticketCodec,
      GatewayConfig config,
      BufferAllocator allocator,
      io.micrometer.core.instrument.MeterRegistry metrics) {
    this.orchestrator = orchestrator;
    this.objects = objects;
    this.ticketCodec = ticketCodec;
    this.config = config;
    this.allocator = allocator;
    this.metrics = metrics;
  }

  /** 错误回写统一汇点（D26）：计数 fg.relay.failures{status} 后经 listener 上报。 */
  private void fail(ServerStreamListener listener, CallStatus status, String description) {
    metrics.counter("fg.relay.failures", "status", status.code().name()).increment();
    listener.error(status.withDescription(description).toRuntimeException());
  }

  void relay(String user, Ticket ticket, ServerStreamListener listener) {
    RelayTicket t;
    try {
      t = ticketCodec.decode(new String(ticket.getBytes(), java.nio.charset.StandardCharsets.UTF_8), user);
    } catch (RelayTicketCodec.ExpiredTicketException e) {
      fail(listener, CallStatus.INVALID_ARGUMENT, "Ticket expired; re-poll for fresh endpoints");
      return;
    } catch (Exception e) {
      fail(listener, CallStatus.UNAUTHENTICATED, "Invalid ticket: " + e.getMessage());
      return;
    }
    metrics.counter("fg.relay.streams", "kind", t.kind().name()).increment();

    // 在途等待，预算按票 kind 分策略（D15/D18/设计 §4.7）：
    //   STREAM（legacy GetFlightInfo 快返票）——票可能在查询 RUNNING 时就到 DoGet，且 legacy
    //     客户端没有 poll 循环可退避，等待预算 = fg.query.timeout（D9 唯一护栏），挂满查询全程；
    //   COMMAND（非 SELECT 快返票）——同 STREAM：legacy 快返的命令票无 poll 侧兜底可退避，
    //     挂满命令全程（fg.command.timeout 先行熔断 + sweeper 兜底，此预算只是上限）；
    //   PART（poll 终态票）——铸造时行已 CAS 终态（单向），到达 DoGet 时必然 COMPLETED，
    //     零等待；若见 RUNNING 即异常态，快速 UNAVAILABLE 暴露而非长等掩盖。
    // 长等待传入 listener::isCancelled：客户端断流后在一个 fg.poll.db.interval 内释放
    // 有界 relay 池线程（否则 600s 预算下废弃流会占满池）。
    Duration waitBudget =
        t.kind() == TicketKind.PART
            ? Duration.ZERO
            : Duration.ofMillis(config.getDurationMs("fg.query.timeout"));
    QueryOrchestrator.PollOutcome outcome;
    try {
      outcome = orchestrator.poll(t.queryId(), waitBudget, listener::isCancelled);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return; // 客户端已断流，流已死，无需回写
    } catch (Exception e) {
      fail(listener, CallStatus.INTERNAL, "Poll failed: " + e.getMessage());
      return;
    }
    if (!outcome.done()) {
      fail(listener, CallStatus.UNAVAILABLE, "Result not ready yet; re-poll");
      return;
    }
    OperationRow row = outcome.row();
    switch (row.status()) {
      case COMPLETED -> { /* fall through to stream */ }
      case CANCELLED -> {
          fail(listener, CallStatus.CANCELLED, "Query was cancelled");
          return;
        }
      default -> {
          fail(listener, CallStatus.INTERNAL,
              "Query failed: " + (row.error() == null ? row.status() : row.error()));
          return;
        }
    }

    if (t.kind() == TicketKind.COMMAND) {
      relayCommand(t, row, listener);
      return;
    }

    ResultManifest manifest = outcome.manifest();
    if (manifest == null) {
      fail(listener, CallStatus.INTERNAL, "Manifest missing for completed query");
      return;
    }

    List<ResultManifest.Part> parts = manifest.parts();
    if (t.kind() == TicketKind.PART) {
      parts =
          parts.stream()
              .filter(p -> p.index() == t.partIndex())
              .toList();
      if (parts.isEmpty()) {
          fail(listener, CallStatus.NOT_FOUND, "Unknown partIndex: " + t.partIndex());
          return;
        }
    }

    BufferAllocator child = allocator.newChildAllocator("fg-relay-" + t.queryId(), 0, Long.MAX_VALUE);
    Pump pump = new Pump();
    try {
      for (ResultManifest.Part part : parts) {
        try (InputStream in = objects.getObject(ObjectStoreService.objectName(part.uri()));
            ArrowStreamReader reader = new ArrowStreamReader(in, child)) {
          pumpReader(reader, pump, child, listener, t.queryId());
        }
      }
      logger.info("Relay complete: {} parts={} rows={}", t.queryId(), parts.size(), pump.rows);
      listener.completed();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail(listener, CallStatus.INTERNAL, "Relay interrupted");
    } catch (Exception e) {
      logger.warn("Relay failed for {}: {}", t.queryId(), e.toString());
      fail(listener, CallStatus.INTERNAL, "Relay failed: " + e.getMessage());
    } finally {
      closePump(pump, child);
    }
  }

  // ------------------------------------------------------------- COMMAND（D18）

  /** 命令结果内联交付：command_result（完整 Arrow IPC stream bytes）经共享泵直灌客户端。 */
  private void relayCommand(RelayTicket t, OperationRow row, ServerStreamListener listener) {
    byte[] result = row.commandResult();
    if (result == null) {
      // COMPLETED 但行内无结果：终态 CAS 与读行之间的异常态（或行被外部改动）
      fail(listener, CallStatus.INTERNAL, "Command result missing for completed command");
      return;
    }
    BufferAllocator child = allocator.newChildAllocator("fg-relay-" + t.queryId(), 0, Long.MAX_VALUE);
    Pump pump = new Pump();
    try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(result), child)) {
      pumpReader(reader, pump, child, listener, t.queryId());
      logger.info("Relay complete: {} command rows={}", t.queryId(), pump.rows);
      listener.completed();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail(listener, CallStatus.INTERNAL, "Relay interrupted");
    } catch (Exception e) {
      logger.warn("Relay failed for {}: {}", t.queryId(), e.toString());
      fail(listener, CallStatus.INTERNAL, "Relay failed: " + e.getMessage());
    } finally {
      closePump(pump, child);
    }
  }

  // ------------------------------------------------------------- 共享流式泵

  /** 输出状态：单一输出 root（putNext 序列化的是 start() 注册的实例）+ 累计行数。 */
  private static final class Pump {
    VectorSchemaRoot out;
    VectorLoader loader;
    long rows;
  }

  /**
   * 逐批装载 + 背压 putNext（STREAM/PART/COMMAND 共享）：reader 的每批经
   * VectorUnloader/VectorLoader 灌入输出 root；首个 reader 确立输出 root 与 listener.start。
   */
  private void pumpReader(
      ArrowStreamReader reader,
      Pump pump,
      BufferAllocator child,
      ServerStreamListener listener,
      String queryId)
      throws Exception {
    VectorSchemaRoot src = reader.getVectorSchemaRoot();
    if (pump.out == null) {
      pump.out = VectorSchemaRoot.create(src.getSchema(), child);
      listener.start(pump.out);
      pump.loader = new VectorLoader(pump.out);
    }
    long readinessTimeoutMs = config.getDurationMs("fg.relay.client.readiness.timeout");
    while (reader.loadNextBatch()) {
      ArrowRecordBatch batch = new VectorUnloader(src).getRecordBatch();
      try {
        pump.loader.load(batch);
      } finally {
        batch.close();
      }
      putNextWhenClientReady(listener, readinessTimeoutMs, queryId);
      pump.rows += pump.out.getRowCount();
    }
  }

  private static void closePump(Pump pump, BufferAllocator child) {
    if (pump.out != null) {
      pump.out.close();
    }
    child.close();
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
