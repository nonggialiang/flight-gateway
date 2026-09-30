package org.fg.frontend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.fg.common.config.GatewayConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * 多 batch 泵 + 流级背压闸门（2026-09-30 DBeaver 实证后续）：多 batch 逐批 putNext、
 * 客户端中途 not-ready（模拟 DBeaver 翻页停留——HTTP/2 窗口不重开）经 onReady 唤醒恢复、
 * 持续不就绪界超时 fail、handler 单次注册复用全程。
 */
class ResultRelayPumpTest {

  private static final String[] KEYS = {"fg.relay.client.readiness.timeout"};

  @BeforeEach
  @AfterEach
  void resetProps() {
    for (String k : KEYS) {
      System.clearProperty(k);
    }
  }

  /** 可编程假 listener：ready 状态 + 捕获 onReady handler（模拟 gRPC 线程触发）。 */
  static class FakeListener implements org.apache.arrow.flight.FlightProducer.ServerStreamListener {
    volatile boolean ready = true;
    volatile boolean cancelled = false;
    final AtomicReference<Runnable> readyHandler = new AtomicReference<>();
    int putNexts;
    int starts;

    @Override
    public void start(VectorSchemaRoot root) {
      starts++;
    }

    @Override
    public void start(
        VectorSchemaRoot root,
        org.apache.arrow.vector.dictionary.DictionaryProvider dictionaries,
        org.apache.arrow.vector.ipc.message.IpcOption option) {
      starts++;
    }

    @Override
    public void putNext() {
      putNexts++;
    }

    @Override
    public void putNext(org.apache.arrow.memory.ArrowBuf metadata) {
      putNexts++;
    }

    @Override
    public void completed() {}

    @Override
    public void error(Throwable error) {}

    @Override
    public boolean isReady() {
      return ready;
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }

    @Override
    public void setOnReadyHandler(Runnable handler) {
      readyHandler.set(handler);
    }

    @Override
    public void putMetadata(org.apache.arrow.memory.ArrowBuf metadata) {}

    @Override
    public void setOnCancelHandler(Runnable handler) {}
  }

  /** 首个 batch 交付后置 not-ready（模拟客户端读满一页——如 DBeaver 翻页停留）。 */
  static final class NotReadyAfterFirstBatch extends FakeListener {
    @Override
    public void putNext() {
      super.putNext();
      if (putNexts == 1) {
        ready = false;
      }
    }

    @Override
    public void putNext(org.apache.arrow.memory.ArrowBuf metadata) {
      super.putNext(metadata);
      if (putNexts == 1) {
        ready = false;
      }
    }
  }

  /** 生成 N 个 batch 的 IPC stream（每批 rowsPerBatch 行 [id]）。 */
  private static byte[] multiBatchStream(int batches, int rowsPerBatch) throws Exception {
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(
            new org.apache.arrow.vector.types.pojo.Schema(java.util.List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null))), alloc)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IntVector v = (IntVector) root.getVector(0);
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
        writer.start();
        int value = 0;
        for (int b = 0; b < batches; b++) {
          v.allocateNew(rowsPerBatch);
          for (int i = 0; i < rowsPerBatch; i++) {
            v.setSafe(i, value++);
          }
          v.setValueCount(rowsPerBatch);
          root.setRowCount(rowsPerBatch);
          writer.writeBatch();
        }
      }
      return out.toByteArray();
    }
  }

  private ResultRelay relay() {
    return new ResultRelay(null, null, null, GatewayConfig.create(), null, null);
  }

  @Test
  void multiBatchAllDeliveredWhenClientAlwaysReady() throws Exception {
    byte[] stream = multiBatchStream(4, 100);
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(stream), alloc)) {
      FakeListener listener = new FakeListener();
      ResultRelay.Pump pump = new ResultRelay.Pump();
      ResultRelay.ReadyGate gate = new ResultRelay.ReadyGate(listener); // 每流一次（生产同构）
      try {
        relay().pumpReader(reader, pump, alloc, listener, gate, "q1");
        assertThat(listener.putNexts).isEqualTo(4);
        assertThat(pump.rows).isEqualTo(400);
        assertThat(listener.starts).isEqualTo(1);
      } finally {
        if (pump.out != null) {
          pump.out.close();
        }
      }
    }
  }

  @Test
  void midStreamBackpressureReleasedByOnReadyHandler() throws Exception {
    System.setProperty("fg.relay.client.readiness.timeout", "10s");
    byte[] stream = multiBatchStream(3, 50);
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(stream), alloc)) {
      NotReadyAfterFirstBatch listener = new NotReadyAfterFirstBatch();
      ResultRelay gate = relay();
      ResultRelay.Pump pump = new ResultRelay.Pump();
      ResultRelay.ReadyGate readyGate = new ResultRelay.ReadyGate(listener);

      CompletableFuture<Void> done = CompletableFuture.runAsync(() -> {
        try {
          gate.pumpReader(reader, pump, alloc, listener, readyGate, "q2");
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      // batch1 交付后应阻塞在 gate（not-ready）
      Thread.sleep(500);
      assertThat(listener.putNexts).isEqualTo(1);
      assertThat(done).isNotDone();

      // 客户端恢复拉行：gRPC 线程触发 onReady → 唤醒
      listener.ready = true;
      Runnable handler = listener.readyHandler.get();
      assertThat(handler).isNotNull();
      handler.run();

      done.get(10, TimeUnit.SECONDS);
      assertThat(listener.putNexts).isEqualTo(3);
      assertThat(pump.rows).isEqualTo(150);
      if (pump.out != null) {
        pump.out.close();
      }
    }
  }

  @Test
  void persistentNotReadyTimesOut() throws Exception {
    System.setProperty("fg.relay.client.readiness.timeout", "300ms");
    byte[] stream = multiBatchStream(2, 10);
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(stream), alloc)) {
      NotReadyAfterFirstBatch listener = new NotReadyAfterFirstBatch();
      ResultRelay gate = relay();
      ResultRelay.Pump pump = new ResultRelay.Pump();
      ResultRelay.ReadyGate readyGate = new ResultRelay.ReadyGate(listener);
      try {
        assertThatThrownBy(() -> gate.pumpReader(reader, pump, alloc, listener, readyGate, "q3"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("backpressure timeout");
        assertThat(listener.putNexts).isEqualTo(1); // batch1 已交付，batch2 超时
      } finally {
        if (pump.out != null) {
          pump.out.close();
        }
      }
    }
  }
}
