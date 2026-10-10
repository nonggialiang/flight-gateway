package org.fg.frontend;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.compression.CommonsCompressionFactory;
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

  /** D33：结果 part 的 IPC body 压缩（zstd/lz4/none 由 fg.result.compression 决定）——读端 codec 工厂（消息级属性，透明解压；none 对象同样可读）。 */
  private static final org.apache.arrow.vector.compression.CompressionCodec.Factory COMPRESSION =
      CommonsCompressionFactory.INSTANCE;

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
    relay(user, ticket, listener, null);
  }

  void relay(
      String user,
      Ticket ticket,
      ServerStreamListener listener,
      PagingMiddleware.PageRequest paging) {
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

    boolean paged = t.kind() == TicketKind.STREAM && paging != null && paging.offset() != null;
    if (paged) {
      metrics.counter("fg.scroll.pages").increment(); // D27：页 DoGet 量（驱动翻页频率）
    }
    // 在途等待，预算按票 kind 分策略（D15/D18/设计 §4.7）：
    //   STREAM（legacy GetFlightInfo 快返票）——票可能在查询 RUNNING 时就到 DoGet，且 legacy
    //     客户端没有 poll 循环可退避，等待预算 = fg.query.timeout（D9 唯一护栏），挂满查询全程；
    //   COMMAND（非 SELECT 快返票）——同 STREAM：legacy 快返的命令票无 poll 侧兜底可退避，
    //     挂满命令全程（fg.command.timeout 先行熔断 + sweeper 兜底，此预算只是上限）；
    //   PART（poll 终态票）——铸造时行已 CAS 终态（单向），到达 DoGet 时必然 COMPLETED，
    //     零等待；若见 RUNNING 即异常态，快速 UNAVAILABLE 暴露而非长等掩盖。
    //   分页 STREAM（D27）——同 PART：页只服务终态数据（驱动先拿 total_records 才翻页），
    //     RUNNING 即 UNAVAILABLE，不占 relay 池长挂。
    // 长等待传入 listener::isCancelled：客户端断流后在一个 fg.poll.db.interval 内释放
    // 有界 relay 池线程（否则 600s 预算下废弃流会占满池）。
    Duration waitBudget =
        t.kind() == TicketKind.PART || paged
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

    // D27：分页 STREAM——页切片（row 级 [offset, offset+limit)），无头路径不进此分支
    if (paged) {
      scrollPage(t, manifest, paging, listener);
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
    ReadyGate gate = new ReadyGate(listener);
    try {
      for (ResultManifest.Part part : parts) {
        try (InputStream in = objects.getObject(ObjectStoreService.objectName(part.uri()));
            ArrowStreamReader reader = new ArrowStreamReader(in, child, COMPRESSION)) {
          pumpReader(reader, pump, child, listener, gate, t.queryId());
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

  // ------------------------------------------------------------- D27 分页 STREAM（scroll）

  /**
   * 页切片：row 级 {@code [offset, offset+limit)}。两级收敛（Dremio loadJobData 同构）：
   * manifest per-part 行数选相交 part（不相交 part 不打开）→ per-part .bidx 选相交 batch →
   * Range GET 取单 encapsulated message →（schema message + 该 message + EOS 的 mini stream
   * 解码——ArrowStreamReader 不能直读裸 batch 切片）→ 首尾 batch 经 splitAndTransfer 裁行。
   * .bidx 缺席/损坏回落整 part 顺序读（优化非正确性依赖）。clamp：limit 缺省
   * {@code fg.result.page.default-rows}、上限 {@code fg.result.page.max-rows}；offset ≥ 总行数
   * 返回空页（非空 schema 承诺照旧）。
   */
  private void scrollPage(
      RelayTicket t, ResultManifest manifest, PagingMiddleware.PageRequest paging,
      ServerStreamListener listener) {
    long defaultRows = config.getInt("fg.result.page.default-rows");
    long maxRows = config.getInt("fg.result.page.max-rows");
    long offset = paging.offset();
    long limit = paging.limit() == null || paging.limit() <= 0
        ? defaultRows : Math.min(paging.limit(), maxRows);

    List<ResultManifest.Part> parts = manifest.parts();
    long[] partRows = parts.stream().mapToLong(ResultManifest.Part::recordCount).toArray();
    long total = manifest.rowCount();

    byte[] schemaMessage = java.util.Base64.getDecoder().decode(manifest.schemaBase64());
    BufferAllocator child = allocator.newChildAllocator("fg-page-" + t.queryId(), 0, Long.MAX_VALUE);
    VectorSchemaRoot out = null;
    try {
      org.apache.arrow.vector.types.pojo.Schema schema = readSchema(schemaMessage, child);
      out = VectorSchemaRoot.create(schema, child);
      listener.start(out);

      long emitted = 0;
      if (offset < total) {
        long remaining = limit;
        for (ScrollPagePlanner.PartSlice ps : ScrollPagePlanner.planParts(partRows, offset, limit)) {
          ResultManifest.Part part = parts.get(ps.partIndex());
          String objectKey = ObjectStoreService.objectName(part.uri());
          org.fg.result.manifest.BatchIndex bidx = objects.readBatchIndex(objectKey);
          if (bidx != null) {
            for (ScrollPagePlanner.BatchSlice bs
                : ScrollPagePlanner.planBatches(bidx, ps.offset(), ps.rows())) {
              org.fg.result.manifest.BatchIndex.Batch b = bidx.batches().get(bs.batchIndex());
              byte[] message = readRange(objectKey, b.offset(), (int) b.length());
              try (ArrowStreamReader reader = miniStream(schemaMessage, message, child)) {
                if (!reader.loadNextBatch()) {
                  throw new IllegalStateException(
                      "batch index stale for " + objectKey + " batch#" + bs.batchIndex());
                }
                VectorSchemaRoot src = reader.getVectorSchemaRoot();
                emitted += appendSlice(src, bs.start(), bs.rows(), out);
                remaining -= bs.rows();
              }
            }
          } else {
            // fallback：整 part 顺序读，行区间裁剪（.bidx 缺席/损坏）
            emitted += relayFallbackRange(
                objectKey, ps.offset(), ps.rows(), out, t.queryId(), child);
            remaining -= ps.rows();
          }
          if (remaining <= 0) {
            break;
          }
        }
      }
      logger.info("Scroll page: {} offset={} limit={} total={} emitted={}",
          t.queryId(), offset, limit, total, emitted);
      if (out.getRowCount() > 0) {
        // 单页单 batch：驱动以整页为装载单元（页 LRU 缓存的原子粒度）；空页只 start+completed
        ReadyGate gate = new ReadyGate(listener);
        gate.await(config.getDurationMs("fg.relay.client.readiness.timeout"), t.queryId());
        listener.putNext();
      }
      listener.completed();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      fail(listener, CallStatus.INTERNAL, "Page relay interrupted");
    } catch (Exception e) {
      logger.warn("Scroll page failed for {}: {}", t.queryId(), e.toString());
      fail(listener, CallStatus.INTERNAL, "Scroll page failed: " + e.getMessage());
    } finally {
      if (out != null) {
        out.close();
      }
      child.close();
    }
  }

  /** fallback 路径：整 part 顺序读，仅装载 [offset, offset+rows) 行。 */
  private long relayFallbackRange(
      String objectKey,
      long offset,
      long rows,
      VectorSchemaRoot out,
      String queryId,
      BufferAllocator child)
      throws Exception {
    long emitted = 0;
    long cursor = 0;
    long need = rows;
    try (InputStream in = objects.getObject(objectKey);
        ArrowStreamReader reader = new ArrowStreamReader(in, child, COMPRESSION)) {
      VectorSchemaRoot src = reader.getVectorSchemaRoot();
      while (need > 0 && reader.loadNextBatch()) {
        int batchRows = src.getRowCount();
        long batchStart = cursor;
        cursor += batchRows;
        long sliceStart = Math.max(offset, batchStart);
        long sliceEnd = Math.min(offset + rows, cursor);
        if (sliceEnd > sliceStart) {
          emitted += appendSlice(src, sliceStart - batchStart, sliceEnd - sliceStart, out);
          need -= sliceEnd - sliceStart;
        }
      }
    }
    return emitted;
  }

  /**
   * 把 src 的行区间 [start, start+count) 追加到输出 root 末尾（copyValueSafe 逐行，容量自扩）。
   * 多个 slice 追加成一页后由调用方单次 putNext（单页单 batch 契约，D27-c 驱动页装载单元）。
   */
  private long appendSlice(VectorSchemaRoot src, long start, long count, VectorSchemaRoot out) {
    if (count <= 0) {
      return 0;
    }
    int base = out.getRowCount();
    for (org.apache.arrow.vector.types.pojo.Field field : src.getSchema().getFields()) {
      org.apache.arrow.vector.ValueVector from = src.getVector(field.getName());
      org.apache.arrow.vector.ValueVector to = out.getVector(field.getName());
      org.apache.arrow.vector.util.TransferPair tp = from.makeTransferPair(to);
      for (int i = 0; i < count; i++) {
        tp.copyValueSafe((int) (start + i), base + i);
      }
    }
    out.setRowCount(base + (int) count);
    return count;
  }

  /** Range GET 读取单 encapsulated message 字节（D28：经 RangeCache 两级缓存）。 */
  private byte[] readRange(String objectKey, long offset, int length) throws Exception {
    return objects.readRange(objectKey, offset, length).join();
  }

  /** schema message + 单 batch message + EOS → ArrowStreamReader（可直读的 mini stream）。 */
  private ArrowStreamReader miniStream(byte[] schemaMessage, byte[] message, BufferAllocator a) {
    java.io.ByteArrayOutputStream mini = new java.io.ByteArrayOutputStream();
    byte[] eos = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0, 0, 0, 0};
    mini.writeBytes(schemaMessage);
    mini.writeBytes(message);
    mini.write(eos, 0, eos.length);
    return new ArrowStreamReader(new java.io.ByteArrayInputStream(mini.toByteArray()), a, COMPRESSION);
  }

  /** manifest.schemaBase64（schema IPC message）→ Schema。 */
  private org.apache.arrow.vector.types.pojo.Schema readSchema(
      byte[] schemaMessage, BufferAllocator a) throws java.io.IOException {
    try (ArrowStreamReader reader = miniStream(schemaMessage, new byte[0], a)) {
      return reader.getVectorSchemaRoot().getSchema();
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
    ReadyGate gate = new ReadyGate(listener);
    try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(result), child)) {
      pumpReader(reader, pump, child, listener, gate, t.queryId());
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
  static final class Pump {
    VectorSchemaRoot out;
    VectorLoader loader;
    long rows;
  }

  /**
   * 逐批装载 + 背压 putNext（STREAM/PART/COMMAND 共享）：reader 的每批经
   * VectorUnloader/VectorLoader 灌入输出 root；首个 reader 确立输出 root 与 listener.start。
   */
  void pumpReader(
      ArrowStreamReader reader,
      Pump pump,
      BufferAllocator child,
      ServerStreamListener listener,
      ReadyGate gate,
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
      gate.await(readinessTimeoutMs, queryId);
      listener.putNext();
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
   * 流级背压闸门（Dremio BackpressureStrategy 骨架，2026-09-30 DBeaver 实证重构）：
   * <b>每条流注册一次 onReady handler、单一 monitor 复用到流结束</b>——此前每 batch 重注册
   * 新 monitor，既有 handler 覆写竞态（gRPC 线程可能持旧 handler 在跑），也依赖 onReady
   * 仅在 not-ready→ready 跃迁触发、注册时已 ready 不回调的语义（1s isReady 轮询兜底）。
   *
   * <p>两个实证教训（保留给后人）：① 回调在 gRPC serializing executor 线程执行，
   * <b>必须持锁 notify</b>——裸 {@code monitor::notifyAll} 即 IllegalMonitorStateException，
   * 唤醒丢失且异常炸进 gRPC 破坏 listener 状态机（error() 不再可达客户端，RPC 悬死）；
   * ② 等待本身是<b>合法背压而非故障</b>：客户端（如 DBeaver 翻页浏览）停止拉行时
   * HTTP/2 窗口不重开、isReady 恒 false——客户端一拉行即恢复；界由
   * {@code fg.relay.client.readiness.timeout} 兜底（超时 fail，防死客户端占住 relay 线程）。
   */
  static final class ReadyGate {

    private final ServerStreamListener listener;
    private final Object monitor = new Object();

    ReadyGate(ServerStreamListener listener) {
      this.listener = listener;
      listener.setOnReadyHandler(() -> {
        synchronized (monitor) {
          monitor.notifyAll();
        }
      });
    }

    /** 等待客户端就绪：onReady 即时唤醒 + 1s isReady 轮询兜底；界超时。 */
    void await(long timeoutMs, String queryId) throws InterruptedException {
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
    }
  }
}
