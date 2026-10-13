package org.fg.frontend;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.jupiter.api.Test;

/**
 * D33 泄漏复现：scrollPage 管线（readSchema + 逐 batch miniStream + appendSlice）在压缩
 * part 上的 child allocator 关闭泄漏检测。复刻 ResultRelay 私有逻辑（miniStream/readSchema/
 * appendSlice 逐行等价），任何 2048B 级泄漏会在 child.close() 处炸 IllegalStateException。
 */
class ScrollPageLeakTest {

  private static final org.apache.arrow.vector.compression.CompressionCodec.Factory COMPRESSION =
      CommonsCompressionFactory.INSTANCE;

  // ---------- 与 ResultRelay 逐行等价的复刻 ----------

  private ArrowStreamReader miniStream(byte[] schemaMessage, byte[] message, BufferAllocator a) {
    ByteArrayOutputStream mini = new ByteArrayOutputStream();
    byte[] eos = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0, 0, 0, 0};
    mini.writeBytes(schemaMessage);
    mini.writeBytes(message);
    mini.write(eos, 0, eos.length);
    return new ArrowStreamReader(new ByteArrayInputStream(mini.toByteArray()), a, COMPRESSION);
  }

  private Schema readSchema(byte[] schemaMessage, BufferAllocator a) throws Exception {
    try (ArrowStreamReader reader = miniStream(schemaMessage, new byte[0], a)) {
      return reader.getVectorSchemaRoot().getSchema();
    }
  }

  private long appendSlice(VectorSchemaRoot src, long start, long count, VectorSchemaRoot out) {
    if (count <= 0) {
      return 0;
    }
    int base = out.getRowCount();
    for (Field field : src.getSchema().getFields()) {
      org.apache.arrow.vector.ValueVector from = src.getVector(field.getName());
      org.apache.arrow.vector.ValueVector to = out.getVector(field.getName());
      TransferPair tp = from.makeTransferPair(to);
      for (int i = 0; i < count; i++) {
        tp.copyValueSafe((int) (start + i), base + i);
      }
    }
    out.setRowCount(base + (int) count);
    return count;
  }

  // ---------- 压缩 part 生成（与 FgArrowPartWriter 同构） ----------

  static final class FramedStream {
    byte[] schemaMessage;
    List<byte[]> batchMessages = new ArrayList<>();
  }

  /** zstd 压缩 IPC stream：schema + batches.length 个 batch + EOS；含 null 行。 */
  private FramedStream compressedStream(int[][] batches) throws Exception {
    Schema schema = new Schema(java.util.List.of(
        new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
      IntVector id = (IntVector) root.getVector("id");
      VarCharVector name = (VarCharVector) root.getVector("name");
      try (ArrowStreamWriter writer = new ArrowStreamWriter(
          root, null,
          java.nio.channels.Channels.newChannel(out),
          IpcOption.DEFAULT, CommonsCompressionFactory.INSTANCE,
          CompressionUtil.CodecType.ZSTD)) {
        writer.start();
        int value = 0;
        for (int[] spec : batches) {
          int rows = spec[0];
          int nullEvery = spec[1]; // 0=无 null；否则每 nullEvery 行置 null
          id.allocateNew(rows);
          name.allocateNew(rows);
          for (int i = 0; i < rows; i++) {
            boolean isNull = nullEvery > 0 && i % nullEvery == 0;
            if (isNull) {
              id.setNull(i);
              name.setNull(i);
            } else {
              id.setSafe(i, value++);
              name.setSafe(i, ("row-" + value).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
          }
          id.setValueCount(rows);
          name.setValueCount(rows);
          root.setRowCount(rows);
          writer.writeBatch();
        }
      }
    }
    return frameMessages(out.toByteArray());
  }

  /** 解 framing：continuation+len+metadata(8 对齐)+body(8 对齐)。 */
  private FramedStream frameMessages(byte[] stream) {
    FramedStream framed = new FramedStream();
    int pos = 0;
    while (pos < stream.length) {
      int prefix = readInt32LE(stream, pos);
      if (prefix == -1) { // continuation
        int metaLen = readInt32LE(stream, pos + 4);
        if (metaLen == 0) {
          break; // EOS
        }
        ByteBuffer metadata = ByteBuffer.wrap(stream, pos + 8, metaLen);
        org.apache.arrow.flatbuf.Message msg =
            org.apache.arrow.flatbuf.Message.getRootAsMessage(metadata);
        long bodyLen = msg.bodyLength();
        long paddedBody = (bodyLen + 7) & ~7L;
        int end = pos + 8 + metaLen + (int) paddedBody;
        byte[] message = java.util.Arrays.copyOfRange(stream, pos, end);
        if (msg.headerType() == org.apache.arrow.flatbuf.MessageHeader.Schema) {
          framed.schemaMessage = message;
        } else {
          framed.batchMessages.add(message);
        }
        pos = end;
      } else {
        break;
      }
    }
    assertThat(framed.schemaMessage).isNotNull();
    return framed;
  }

  private static int readInt32LE(byte[] b, int off) {
    return (b[off] & 0xFF) | ((b[off + 1] & 0xFF) << 8) | ((b[off + 2] & 0xFF) << 16)
        | ((b[off + 3] & 0xFF) << 24);
  }

  // ---------- 复现用例 ----------

  private void runScrollPipeline(FramedStream framed, long offset, long limit) {
    try (RootAllocator root = new RootAllocator(Long.MAX_VALUE)) {
      BufferAllocator child = root.newChildAllocator("fg-page-test", 0, Long.MAX_VALUE);
      VectorSchemaRoot out = null;
      try {
        Schema schema = readSchema(framed.schemaMessage, child);
        out = VectorSchemaRoot.create(schema, child);
        long cursor = 0; // 已消费的全局行游标（scrollPage 同构）
        long remaining = limit;
        for (byte[] message : framed.batchMessages) {
          if (remaining <= 0) {
            break;
          }
          try (ArrowStreamReader reader = miniStream(framed.schemaMessage, message, child)) {
            if (!reader.loadNextBatch()) {
              throw new IllegalStateException("batch index stale");
            }
            VectorSchemaRoot src = reader.getVectorSchemaRoot();
            int rows = src.getRowCount();
            long sliceStart = Math.max(offset, cursor);
            long sliceEnd = Math.min(offset + limit, cursor + rows);
            if (sliceEnd > sliceStart) {
              appendSlice(src, sliceStart - cursor, sliceEnd - sliceStart, out);
              remaining -= sliceEnd - sliceStart;
            }
            cursor += rows;
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        if (out != null) {
          out.close();
        }
        child.close(); // 泄漏在此引爆
      }
    }
  }

  @Test
  void zstdNormalBatchesNoLeak() throws Exception {
    runScrollPipeline(compressedStream(new int[][] {{500, 3}, {500, 0}, {500, 5}}), 0, 1500);
  }

  @Test
  void zstdAllNullBatchNoLeak() throws Exception {
    runScrollPipeline(compressedStream(new int[][] {{10, 1}}), 0, 10);
  }

  @Test
  void zstdTinyBatchNoLeak() throws Exception {
    runScrollPipeline(compressedStream(new int[][] {{1, 0}, {2, 2}}), 0, 3);
  }

  /** 生产同构：NOT NULL 字段（0 长 validity → 读端 loadValidityBuffer case B 新分配）+ 16384 行批 + 页中切片。 */
  @Test
  void zstdNotNullMidBatchSliceNoLeak() throws Exception {
    FramedStream framed = notNullHeavyStream(new int[] {16384, 16384});
    // 第二批中段起取 1000 行（scroll 页默认）
    runScrollPipeline(framed, 16384 + 500, 1000);
    // 首批中段
    runScrollPipeline(framed, 8000, 1000);
    // 跨批页
    runScrollPipeline(framed, 16000, 2000);
  }

  /** NOT NULL int + NOT NULL bigint + nullable varchar，16384 行/批，varchar 稀疏 null。 */
  private FramedStream notNullHeavyStream(int[] batches) throws Exception {
    Schema schema = new Schema(java.util.List.of(
        new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
        new Field("v", FieldType.notNullable(new ArrowType.Int(32, true)), null),
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
      org.apache.arrow.vector.BigIntVector id =
          (org.apache.arrow.vector.BigIntVector) root.getVector("id");
      IntVector v = (IntVector) root.getVector("v");
      VarCharVector name = (VarCharVector) root.getVector("name");
      try (ArrowStreamWriter writer = new ArrowStreamWriter(
          root, null,
          java.nio.channels.Channels.newChannel(out),
          IpcOption.DEFAULT, CommonsCompressionFactory.INSTANCE,
          CompressionUtil.CodecType.ZSTD)) {
        writer.start();
        long value = 0;
        for (int rows : batches) {
          id.allocateNew(rows);
          v.allocateNew(rows);
          name.allocateNew(rows);
          for (int i = 0; i < rows; i++) {
            id.setSafe(i, value);
            v.setSafe(i, (int) value);
            if (i % 7 == 0) {
              name.setNull(i);
            } else {
              name.setSafe(i, ("row-" + value).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            value++;
          }
          id.setValueCount(rows);
          v.setValueCount(rows);
          name.setValueCount(rows);
          root.setRowCount(rows);
          writer.writeBatch();
        }
      }
    }
    return frameMessages(out.toByteArray());
  }


  @Test
  void schemaReadAloneNoLeak() throws Exception {
    FramedStream framed = compressedStream(new int[][] {{100, 0}});
    try (RootAllocator root = new RootAllocator(Long.MAX_VALUE)) {
      BufferAllocator child = root.newChildAllocator("fg-page-test", 0, Long.MAX_VALUE);
      try {
        readSchema(framed.schemaMessage, child);
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        child.close();
      }
    }
  }

  /** fallback 路径：整 part 顺序读（relayFallbackRange 复刻）。 */
  @Test
  void fallbackFullReadNoLeak() throws Exception {
    int[][] spec = {{300, 4}, {300, 0}};
    Schema schema = new Schema(java.util.List.of(
        new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
    ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    try (RootAllocator rootAlloc = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, rootAlloc)) {
      IntVector id = (IntVector) root.getVector("id");
      VarCharVector name = (VarCharVector) root.getVector("name");
      try (ArrowStreamWriter writer = new ArrowStreamWriter(
          root, null,
          java.nio.channels.Channels.newChannel(outBytes),
          IpcOption.DEFAULT, CommonsCompressionFactory.INSTANCE,
          CompressionUtil.CodecType.ZSTD)) {
        writer.start();
        int value = 0;
        for (int[] s : spec) {
          id.allocateNew(s[0]);
          name.allocateNew(s[0]);
          for (int i = 0; i < s[0]; i++) {
            if (s[1] > 0 && i % s[1] == 0) {
              id.setNull(i);
              name.setNull(i);
            } else {
              id.setSafe(i, value++);
              name.setSafe(i, ("row-" + value).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
          }
          id.setValueCount(s[0]);
          name.setValueCount(s[0]);
          root.setRowCount(s[0]);
          writer.writeBatch();
        }
      }
      byte[] stream = outBytes.toByteArray();
      {
        BufferAllocator child = rootAlloc.newChildAllocator("fg-page-test", 0, Long.MAX_VALUE);
        VectorSchemaRoot out = null;
        try {
          // 整流读（relayFallbackRange 同构）
          try (ArrowStreamReader reader = new ArrowStreamReader(
              new ByteArrayInputStream(stream), child, COMPRESSION)) {
            VectorSchemaRoot src = reader.getVectorSchemaRoot();
            Schema srcSchema = src.getSchema();
            out = VectorSchemaRoot.create(srcSchema, child);
            while (reader.loadNextBatch()) {
              appendSlice(src, 0, src.getRowCount(), out);
            }
          }
        } finally {
          if (out != null) {
            out.close();
          }
          child.close();
        }
      }
    }
  }

  // ---------- 前缀损坏（上游 apache/arrow-java#1116 写端 bug 产物）读端机制复现 ----------

  /**
   * 写端挂"损坏工厂"：zstd 压缩后把 8 字节未压缩长度前缀写成 0（上游 #1116 观测到的产物：
   * 前缀 0 而 zstd frame 头声明 27752 字节内容）。读端 19.0.0 读到前缀 0 → 走 empty
   * shortcut → loadFieldBuffers 中途炸 → VectorLoader catch 不关 ownBuffers → child.close()
   * 报泄漏（掩盖原始异常）。
   */
  @Test
  void prefixCorruptedStreamReproducesLeak() throws Exception {
    Schema schema = new Schema(java.util.List.of(
        new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    org.apache.arrow.vector.compression.CompressionCodec.Factory corrupting =
        new org.apache.arrow.vector.compression.CompressionCodec.Factory() {
          final org.apache.arrow.vector.compression.CompressionCodec.Factory delegate =
              CommonsCompressionFactory.INSTANCE;

          @Override
          public org.apache.arrow.vector.compression.CompressionCodec createCodec(
              CompressionUtil.CodecType codecType) {
            org.apache.arrow.vector.compression.CompressionCodec inner =
                delegate.createCodec(codecType);
            return new org.apache.arrow.vector.compression.CompressionCodec() {
              @Override
              public org.apache.arrow.memory.ArrowBuf compress(
                  org.apache.arrow.memory.BufferAllocator a,
                  org.apache.arrow.memory.ArrowBuf uncompressed) {
                org.apache.arrow.memory.ArrowBuf compressed = inner.compress(a, uncompressed);
                if (compressed.writerIndex() > 64) { // 只损坏"真压缩"的 buffer（跳过空/raw）
                  compressed.setLong(0, 0L); // ← #1116：前缀 0 + 真 zstd frame
                }
                return compressed;
              }

              @Override
              public org.apache.arrow.memory.ArrowBuf decompress(
                  org.apache.arrow.memory.BufferAllocator a,
                  org.apache.arrow.memory.ArrowBuf compressed) {
                return inner.decompress(a, compressed);
              }

              @Override
              public CompressionUtil.CodecType getCodecType() {
                return inner.getCodecType();
              }
            };
          }

          @Override
          public org.apache.arrow.vector.compression.CompressionCodec createCodec(
              CompressionUtil.CodecType codecType, int level) {
            return createCodec(codecType);
          }
        };
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
      org.apache.arrow.vector.BigIntVector id =
          (org.apache.arrow.vector.BigIntVector) root.getVector("id");
      VarCharVector name = (VarCharVector) root.getVector("name");
      int rows = 16384;
      try (ArrowStreamWriter writer = new ArrowStreamWriter(
          root, null,
          java.nio.channels.Channels.newChannel(out),
          IpcOption.DEFAULT, corrupting, CompressionUtil.CodecType.ZSTD)) {
        writer.start();
        id.allocateNew(rows);
        name.allocateNew(rows);
        for (int i = 0; i < rows; i++) {
          id.setSafe(i, i);
          if (i % 7 == 0) {
            name.setNull(i);
          } else {
            name.setSafe(i, ("row-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
          }
        }
        id.setValueCount(rows);
        name.setValueCount(rows);
        root.setRowCount(rows);
        writer.writeBatch();
      }
    }
    FramedStream framed = frameMessages(out.toByteArray());

    // 读端（原版工厂）应复现：loadFieldBuffers 炸 + child.close() 报泄漏（掩盖原始异常）
    RootAllocator root = new RootAllocator(Long.MAX_VALUE);
    try {
      BufferAllocator child = root.newChildAllocator("fg-page-test", 0, Long.MAX_VALUE);
      VectorSchemaRoot outRoot = null;
      Throwable primary = null;
      try {
        Schema schemaRead = readSchema(framed.schemaMessage, child);
        outRoot = VectorSchemaRoot.create(schemaRead, child);
        for (byte[] message : framed.batchMessages) {
          try (ArrowStreamReader reader = miniStream(framed.schemaMessage, message, child)) {
            if (!reader.loadNextBatch()) {
              break;
            }
            appendSlice(reader.getVectorSchemaRoot(), 0,
                reader.getVectorSchemaRoot().getRowCount(), outRoot);
          }
        }
      } catch (Exception e) {
        primary = e;
      } finally {
        if (outRoot != null) {
          outRoot.close();
        }
        Throwable leak = null;
        try {
          child.close();
        } catch (IllegalStateException e) {
          leak = e;
        }
        // 机制断言：原始异常（#1116 形态 IndexOutOfBoundsException）+ 泄漏掩盖
        assertThat(primary).isNotNull();
        assertThat(primary.toString())
            .contains("IndexOutOfBoundsException");
        assertThat(leak).as("child.close 应报泄漏（#1116 产物 + VectorLoader catch 路径）")
            .isNotNull();
      }
    } finally {
      try {
        root.close(); // 泄漏 buffer 未释放（上游 VectorLoader catch 丢弃）——root 关闭预期报错
      } catch (IllegalStateException ignored) {
        // 预期：这正是网关侧无能为力的部分（真上游泄漏），网关只保证不被它掩盖
      }
    }
  }

  /** 同一损坏流 + D34 容错工厂（ResilientCompressionFactory）：应无损读回全部行，child 干净关闭。 */
  @Test
  void corruptStreamRecoveredByResilientFactory() throws Exception {
    // 生成与上例同构的损坏流（前缀 0 + 完好 zstd frame）
    Schema schema = new Schema(java.util.List.of(
        new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    org.apache.arrow.vector.compression.CompressionCodec.Factory corrupting =
        corruptingFactory();
    int rows = 16384;
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
      org.apache.arrow.vector.BigIntVector id =
          (org.apache.arrow.vector.BigIntVector) root.getVector("id");
      VarCharVector name = (VarCharVector) root.getVector("name");
      try (ArrowStreamWriter writer = new ArrowStreamWriter(
          root, null,
          java.nio.channels.Channels.newChannel(out),
          IpcOption.DEFAULT, corrupting, CompressionUtil.CodecType.ZSTD)) {
        writer.start();
        id.allocateNew(rows);
        name.allocateNew(rows);
        for (int i = 0; i < rows; i++) {
          id.setSafe(i, i);
          if (i % 7 == 0) {
            name.setNull(i);
          } else {
            name.setSafe(i, ("row-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
          }
        }
        id.setValueCount(rows);
        name.setValueCount(rows);
        root.setRowCount(rows);
        writer.writeBatch();
      }
    }
    FramedStream framed = frameMessages(out.toByteArray());

    // 容错工厂读回：数据无损（id 逐行核对 + name null 位核对）+ child 干净关闭
    try (RootAllocator rootAlloc = new RootAllocator(Long.MAX_VALUE)) {
      BufferAllocator child = rootAlloc.newChildAllocator("fg-page-test", 0, Long.MAX_VALUE);
      VectorSchemaRoot outRoot = null;
      try {
        try (ArrowStreamReader reader = new ArrowStreamReader(
            new ByteArrayInputStream(framed.schemaMessage),
            child, ResilientCompressionFactory.INSTANCE)) {
          // 先只预热 schema：miniStream 内部 schema 段无 body，直接构造完整流读
        }
        Schema schemaRead = readSchemaResilient(framed.schemaMessage, child);
        outRoot = VectorSchemaRoot.create(schemaRead, child);
        for (byte[] message : framed.batchMessages) {
          org.apache.arrow.vector.compression.CompressionCodec.Factory resilient =
              ResilientCompressionFactory.INSTANCE;
          ByteArrayOutputStream mini = new ByteArrayOutputStream();
          byte[] eos = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0, 0, 0, 0};
          mini.writeBytes(framed.schemaMessage);
          mini.writeBytes(message);
          mini.write(eos, 0, eos.length);
          try (ArrowStreamReader reader = new ArrowStreamReader(
              new ByteArrayInputStream(mini.toByteArray()), child, resilient)) {
            if (!reader.loadNextBatch()) {
              break;
            }
            appendSlice(reader.getVectorSchemaRoot(), 0,
                reader.getVectorSchemaRoot().getRowCount(), outRoot);
          }
        }
        assertThat(outRoot.getRowCount()).isEqualTo(rows);
        org.apache.arrow.vector.BigIntVector id =
            (org.apache.arrow.vector.BigIntVector) outRoot.getVector("id");
        VarCharVector name = (VarCharVector) outRoot.getVector("name");
        for (int i = 0; i < rows; i += 997) { // 抽样核对数据无损
          assertThat(id.get(i)).isEqualTo(i);
          if (i % 7 == 0) {
            assertThat(name.get(i)).isNull();
          } else {
            assertThat(new String(name.get(i), java.nio.charset.StandardCharsets.UTF_8))
                .isEqualTo("row-" + i);
          }
        }
      } finally {
        if (outRoot != null) {
          outRoot.close();
        }
        child.close(); // 容错路径下应干净关闭（无泄漏异常）
      }
    }
  }

  private Schema readSchemaResilient(byte[] schemaMessage, BufferAllocator a) throws Exception {
    ByteArrayOutputStream mini = new ByteArrayOutputStream();
    byte[] eos = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0, 0, 0, 0};
    mini.writeBytes(schemaMessage);
    mini.write(eos, 0, eos.length);
    try (ArrowStreamReader reader = new ArrowStreamReader(
        new ByteArrayInputStream(mini.toByteArray()), a, ResilientCompressionFactory.INSTANCE)) {
      return reader.getVectorSchemaRoot().getSchema();
    }
  }

  /** 上游 #1116 形态损坏工厂：zstd 压缩后把 8 字节前缀清零（保留完好 frame）。 */
  private static org.apache.arrow.vector.compression.CompressionCodec.Factory corruptingFactory() {
    final org.apache.arrow.vector.compression.CompressionCodec.Factory delegate =
        CommonsCompressionFactory.INSTANCE;
    return new org.apache.arrow.vector.compression.CompressionCodec.Factory() {
      @Override
      public org.apache.arrow.vector.compression.CompressionCodec createCodec(
          CompressionUtil.CodecType codecType) {
        org.apache.arrow.vector.compression.CompressionCodec inner =
            delegate.createCodec(codecType);
        return new org.apache.arrow.vector.compression.CompressionCodec() {
          @Override
          public org.apache.arrow.memory.ArrowBuf compress(
              org.apache.arrow.memory.BufferAllocator a,
              org.apache.arrow.memory.ArrowBuf uncompressed) {
            org.apache.arrow.memory.ArrowBuf compressed = inner.compress(a, uncompressed);
            if (compressed.writerIndex() > 64) {
              compressed.setLong(0, 0L);
            }
            return compressed;
          }

          @Override
          public org.apache.arrow.memory.ArrowBuf decompress(
              org.apache.arrow.memory.BufferAllocator a,
              org.apache.arrow.memory.ArrowBuf compressed) {
            return inner.decompress(a, compressed);
          }

          @Override
          public CompressionUtil.CodecType getCodecType() {
            return inner.getCodecType();
          }
        };
      }

      @Override
      public org.apache.arrow.vector.compression.CompressionCodec createCodec(
          CompressionUtil.CodecType codecType, int level) {
        return createCodec(codecType);
      }
    };
  }

  // ---------- 真实 Flight 栈端到端（真 listener.start/putNext/completed + 真客户端拉流） ----------

  /**
   * 进程内 FlightServer：producer.getStream 跑 scroll 管线（readSchema + miniStream + appendSlice
   * + 真 listener 三件套），FlightClient 拉完流后服务端 child.close() —— 泄漏在此引爆。
   */
  @Test
  void realFlightServerScrollPageNoLeak() throws Exception {
    FramedStream framed = notNullHeavyStream(new int[] {16384, 16384});
    List<Throwable> serverErrors = java.util.Collections.synchronizedList(new ArrayList<>());

    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
        org.apache.arrow.flight.FlightServer server =
            org.apache.arrow.flight.FlightServer.builder(
                    alloc, org.apache.arrow.flight.Location.forGrpcInsecure("localhost", 0),
                    new org.apache.arrow.flight.NoOpFlightProducer() {
                      @Override
                      public void getStream(
                          org.apache.arrow.flight.FlightProducer.CallContext context,
                          org.apache.arrow.flight.Ticket ticket,
                          org.apache.arrow.flight.FlightProducer.ServerStreamListener listener) {
                        BufferAllocator child =
                            alloc.newChildAllocator("fg-page-test", 0, Long.MAX_VALUE);
                        VectorSchemaRoot out = null;
                        try {
                          Schema schema = readSchema(framed.schemaMessage, child);
                          out = VectorSchemaRoot.create(schema, child);
                          listener.start(out);
                          long cursor = 0;
                          for (byte[] message : framed.batchMessages) {
                            if (cursor >= 2000) {
                              break;
                            }
                            try (ArrowStreamReader reader =
                                miniStream(framed.schemaMessage, message, child)) {
                              if (!reader.loadNextBatch()) {
                                throw new IllegalStateException("stale");
                              }
                              VectorSchemaRoot src = reader.getVectorSchemaRoot();
                              long take = Math.min(2000, src.getRowCount());
                              appendSlice(src, 0, take, out);
                              cursor += take;
                            }
                          }
                          listener.putNext();
                          listener.completed();
                        } catch (Exception e) {
                          serverErrors.add(e);
                          listener.error(
                              org.apache.arrow.flight.CallStatus.INTERNAL
                                  .withDescription(e.toString())
                                  .toRuntimeException());
                        } finally {
                          if (out != null) {
                            out.close();
                          }
                          child.close();
                        }
                      }
                    })
                .build()
                .start()) {
      try (org.apache.arrow.flight.FlightClient client =
          org.apache.arrow.flight.FlightClient.builder(
                  alloc, org.apache.arrow.flight.Location.forGrpcInsecure(
                      "localhost", server.getPort()))
              .build()) {
        long rows = 0;
        try (org.apache.arrow.flight.FlightStream stream =
            client.getStream(new org.apache.arrow.flight.Ticket(new byte[] {1}))) {
          while (stream.next()) {
            rows += stream.getRoot().getRowCount();
          }
        }
        assertThat(rows).isEqualTo(2000);
      }
      assertThat(serverErrors).isEmpty();
    }
  }
}
