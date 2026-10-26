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
import org.fg.orchestrator.SchemaSerde;
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

  /**
   * D33：结果 part 的 IPC body 压缩（zstd/lz4/none 由 fg.result.compression 决定）——读端
   * codec 工厂（消息级属性，透明解压；none 对象同样可读）。D34：换 {@link ResilientCompressionFactory}
   * ——上游 apache/arrow-java#1116 写端 bug 产出的"前缀 0 + 完好 zstd frame"对象可兜底读回。
   */
  private static final org.apache.arrow.vector.compression.CompressionCodec.Factory COMPRESSION =
      ResilientCompressionFactory.INSTANCE;

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
    relay(user, ticket, listener, null, null);
  }

  void relay(
      String user,
      Ticket ticket,
      ServerStreamListener listener,
      PagingMiddleware.PageRequest paging,
      String lastResponseId) { // D38:INLINE 断线续传游标(null=从头)
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
    // 在途等待，按票 kind 分策略（D15/D18/设计 §4.7；D37 等待全事件驱动）：
    //   STREAM（legacy GetFlightInfo 快返票）——票可能在查询 RUNNING 时就到 DoGet，且 legacy
    //     客户端没有 poll 循环可退避——挂到行级 deadline（created_at + fg.query.timeout，
    //     与 sweeper 同值同时到点）；内部等 attach future，毫秒级唤醒；
    //   COMMAND（非 SELECT 快返票）——同 STREAM：legacy 快返的命令票无 poll 侧兜底可退避，
    //     挂满命令全程（fg.command.timeout 先行熔断 + sweeper 兜底，此预算只是上限）；
    //   PART（poll 终态票）——铸造时行已 CAS 终态（单向），到达 DoGet 时必然 COMPLETED，
    //     零等待；若见 RUNNING 即异常态，快速 UNAVAILABLE 暴露而非长等掩盖。
    //   分页 STREAM（D27）——同 PART：页只服务终态数据（驱动先拿 total_records 才翻页），
    //     RUNNING 即 UNAVAILABLE，不占 relay 池长挂。
    // D37：客户端断流经 setOnCancelHandler → cancelSignal 参与 poll 的 anyOf 等待——
    // 断流毫秒级释放有界 relay 池线程（不再依赖轮询粒度）。
    // D38:inline 分发必须在 poll 之前——终态由消费 DoGet 观测,poll 等终态=死锁。
    // pollOnce 只查行不等待(RUNNING 也可开始消费);终态行为 relayInline 内部处理
    if (t.kind() == TicketKind.INLINE) {
      try {
        // pollOnce 查行不等待;not-done(row=null,RUNNING)时直接查行——RUNNING 即开始消费
        QueryOrchestrator.PollOutcome out = orchestrator.pollOnce(t.queryId());
        org.fg.orchestrator.store.OperationRow inlineRow =
            out.done() ? out.row() : orchestrator.getRow(t.queryId());
        relayInline(t, inlineRow, listener, lastResponseId);
      } catch (Exception e) {
        fail(listener, CallStatus.INTERNAL, "Inline poll failed: " + e.getMessage());
      }
      return;
    }

    boolean zeroWait = t.kind() == TicketKind.PART || paged;
    QueryOrchestrator.PollOutcome outcome;
    if (zeroWait) {
      // PART/页票零等待语义：只查一次，不进入 poll 长等待
      try {
        outcome = orchestrator.pollOnce(t.queryId());
      } catch (Exception e) {
        fail(listener, CallStatus.INTERNAL, "Poll failed: " + e.getMessage());
        return;
      }
    } else {
      java.util.concurrent.CompletableFuture<Void> cancelSignal =
          new java.util.concurrent.CompletableFuture<>();
      listener.setOnCancelHandler(
          () -> cancelSignal.complete(null)); // gRPC 回调线程；complete 线程安全
      try {
        outcome = orchestrator.poll(t.queryId(), cancelSignal);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return; // 客户端已断流，流已死，无需回写
      } catch (Exception e) {
        fail(listener, CallStatus.INTERNAL, "Poll failed: " + e.getMessage());
        return;
      }
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
      closePump(pump, child, t.queryId());
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
      closeChildQuietly(child, "scroll page " + t.queryId());
    }
  }

  /**
   * D34：防御性子分配器关闭。上游 VectorLoader 异常路径（apache/arrow-java#1116 损坏对象 →
   * loadFieldBuffers 抛错）会漏 ownBuffers，child.close() 随之抛"Memory was leaked"并
   * <b>掩盖已回写客户端的原始异常</b>——这里吞掉泄漏异常、原样记日志（原始异常已在上方
   * catch 中 fail() 给客户端 + logger.warn），让 relay 线程干净收尾。
   */
  private static void closeChildQuietly(BufferAllocator child, String context) {
    try {
      child.close();
    } catch (IllegalStateException e) {
      logger.error("Child allocator leaked for {} (upstream #1116 chain; see warn above): {}",
          context, e.toString());
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

  // ------------------------------------------------------------- D38 INLINE（流式取数）

  /** inline 消费互斥：同 queryId 同实例同时至多一个活跃消费（跨实例由引擎 INVALID_CURSOR 裁决）。 */
  private final java.util.Set<String> inlineInFlight = java.util.concurrent.ConcurrentHashMap.newKeySet();

  /**
   * inline 中继：attachInline(lastResponseId) → 引擎响应流逐批转发。schema 用行上
   * schemaBytes（AnalyzePlan 去重后，防引擎流 schema 重名列不一致）；每批
   * {@code putNext(metadata=responseId)}（metadata 直达 FlightData.app_metadata，arrow19
   * 实证）成功后立即 {@code releaseUntil}（前推 earliest_unreleased——断线时续传零丢失）。
   * 断流：不迁行不释放（保留续传窗口），客户端持最后 app_metadata 重连续传；流终：
   * casComplete + releaseAll。
   */
  private void relayInline(
      RelayTicket t, OperationRow row, ServerStreamListener listener, String lastResponseId) {
    if (!inlineInFlight.add(t.queryId())) {
      fail(listener, CallStatus.INVALID_ARGUMENT,
          "Inline stream already active for this query (forward-only cursor)");
      return;
    }
    metrics.counter("fg.relay.streams", "kind", "INLINE").increment();
    BufferAllocator child = allocator.newChildAllocator("fg-inline-" + t.queryId(), 0, Long.MAX_VALUE);
    ReadyGate gate = new ReadyGate(listener);
    VectorSchemaRoot out = null; // D38 泄漏修复:finally 必须关(out 的向量持 child 内存)
    try {
      // 行终态即错误面（inline 票只服务在途消费；终态行给终态错误/空完成）
      if (row.status().terminal()) {
        switch (row.status()) {
          case COMPLETED -> listener.completed(); // 已消费完的续传点：空完成（客户端已收全部）
          case CANCELLED -> fail(listener, CallStatus.CANCELLED, "Query was cancelled");
          default -> fail(listener, CallStatus.INTERNAL,
              "Query failed: " + (row.error() == null ? "unknown" : row.error()));
        }
        return;
      }

      org.apache.arrow.vector.types.pojo.Schema schema = SchemaSerde.deserialize(row.schemaBytes());
      out = VectorSchemaRoot.create(schema, child);
      VectorLoader loader = new VectorLoader(out);
      listener.start(out);
      // 消费回调在 attach onNext 线程执行——装载+putNext+releaseUntil 串行（背压经 gate）

      final java.util.concurrent.atomic.AtomicReference<String> lastAcked =
          new java.util.concurrent.atomic.AtomicReference<>(lastResponseId);

      org.fg.spi.SqlEngine.InlineConsumer consumer =
          new org.fg.spi.SqlEngine.InlineConsumer() {
            @Override
            public void onSchema(org.apache.arrow.vector.types.pojo.Schema engineSchema) {
              // root 已按行上 schema 建（AnalyzePlan 去重后，防引擎流重名列不一致）；
              // 引擎流 schema 仅到达通知，不重建
            }

            @Override
            public void onBatch(String rid, byte[] ipcStreamBytes) {
              // Spark ArrowBatch.data = 自含 Arrow IPC stream（pyspark 同款消费方式）——
              // 解码成 batch 后 VectorLoader 灌入输出 root（行上 schema）
              try (ArrowStreamReader batchReader =
                  new ArrowStreamReader(
                      new java.io.ByteArrayInputStream(ipcStreamBytes), child)) {
                if (!batchReader.loadNextBatch()) {
                  throw new IllegalStateException("inline batch stream empty: " + t.queryId());
                }
                VectorSchemaRoot src = batchReader.getVectorSchemaRoot();
                org.apache.arrow.vector.ipc.message.ArrowRecordBatch batch =
                    new VectorUnloader(src).getRecordBatch();
                try {
                  loader.load(batch);
                } finally {
                  batch.close();
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              try {
                gate.await(config.getDurationMs("fg.relay.client.readiness.timeout"), t.queryId());
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
              // metadata 盖 responseId（SPI onBatch 直参）——直达 FlightData.app_metadata（arrow19 实证）
              org.apache.arrow.memory.ArrowBuf meta = null;
              if (rid != null) {
                meta = child.buffer(rid.length());
                meta.setBytes(0, rid.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    0, rid.length());
                meta.writerIndex(rid.length());
              }
              listener.putNext(meta);
              lastAcked.set(rid);
              orchestrator.releaseInline(row, rid); // putNext 成功即前推（断线续传零丢失）
            }

            @Override
            public void onResultComplete() {
              // 流终仍以 onCompleted 为准（防御性放行）；此处仅记忆
            }
          };

      java.util.concurrent.CompletableFuture<Void> clientGone = new java.util.concurrent.CompletableFuture<>();
      listener.setOnCancelHandler(() -> clientGone.complete(null));
      java.util.concurrent.CompletableFuture<org.fg.spi.ExecutionOutcome> f =
          orchestrator.attachInline(row, lastResponseId, consumer);
      // 注:inline 的 ArrowRecordBatch 构造在 consumer 内((rowCount, bodyBuffers) 两参形态
      // 经 CompressionUtil 兼容——见下 onBatch 实现)

      // 等待:引擎流终 or 客户端断流
      java.util.concurrent.CompletableFuture<Object> either =
          java.util.concurrent.CompletableFuture.anyOf(f, clientGone);
      either.join();
      if (clientGone.isDone() && !f.isDone()) {
        // 客户端断流：不迁行不释放（续传窗口保留）；流留给引擎缓存,由续传或 sweeper 收尾
        logger.info("Inline client disconnected: {} lastAcked={}", t.queryId(), lastAcked.get());
        return;
      }
      org.fg.spi.ExecutionOutcome outcome = f.join();
      switch (outcome.status()) {
        case COMPLETED -> {
          orchestrator.casCompleteInline(t.queryId());
          orchestrator.releaseInlineAll(row);
          listener.completed();
          logger.info("Inline relay complete: {}", t.queryId());
        }
        case CANCELLED -> {
          orchestrator.cancelInline(row); // casCancel + releaseAll（幂等）
          fail(listener, CallStatus.CANCELLED, "Query was cancelled");
        }
        case FAILED -> {
          orchestrator.casFailInline(t.queryId(), outcome.error());
          orchestrator.releaseInlineAll(row);
          CallStatus status = translateInlineError(outcome.error());
          fail(listener, status,
              status == CallStatus.INVALID_ARGUMENT
                  ? "Resume cursor no longer available (data released); re-execute the query"
                  : (outcome.error() == null ? "Inline failed" : outcome.error()));
        }
        case UNKNOWN -> fail(listener, CallStatus.UNAVAILABLE,
            "Inline stream interrupted; resume with x-fg-resume-response-id: "
                + (lastAcked.get() == null ? "(from start)" : lastAcked.get()));
      }
    } catch (Exception e) {
      // D39b 排障：带完整堆栈——此前仅 e.toString() 吞掉了服务端栈，"Not started" 类
      // gRPC 守卫异常无法定位触发点（classpath 字节扫描：grpc-core ClientCallImpl 的
      // request/halfClose/sendMessage pre-start 守卫）
      logger.warn("Inline relay failed for {}", t.queryId(), e);
      fail(listener, CallStatus.INTERNAL, "Inline relay failed: " + e.getMessage());
    } finally {
      inlineInFlight.remove(t.queryId());
      if (out != null) {
        try {
          out.close();
        } catch (Exception e) {
          logger.debug("Inline out close failed for {}: {}", t.queryId(), e.toString());
        }
      }
      closeChildQuietly(child, "inline " + t.queryId());
    }
  }

  /** INVALID_CURSOR 族（字节码实证的 Spark 原生错误）→ 客户端错误面。 */
  private static CallStatus translateInlineError(String error) {
    if (error == null) {
      return CallStatus.INTERNAL;
    }
    if (error.contains("INVALID_CURSOR.POSITION_NOT_AVAILABLE")) {
      return CallStatus.INVALID_ARGUMENT; // 游标已释放（前向单游标协议核心错误）
    }
    if (error.contains("POSITION_NOT_FOUND")
        || error.contains("NOT_REATTACHABLE")
        || error.contains("INVALID_CURSOR.DISCONNECTED")) {
      return CallStatus.INVALID_ARGUMENT;
    }
    if (error.startsWith("operation not found")) {
      return CallStatus.NOT_FOUND;
    }
    return CallStatus.INTERNAL; // 引擎执行错原样透传（fail 分支带 error 文本）
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
      closePump(pump, child, t.queryId());
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

  private static void closePump(Pump pump, BufferAllocator child, String queryId) {
    if (pump.out != null) {
      pump.out.close();
    }
    closeChildQuietly(child, "relay " + queryId); // D34：同 scrollPage，防泄漏异常掩盖原始错误
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
