package org.fg.spark.sink;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.execution.arrow.ArrowWriter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * executor task 写入器：InternalRow → Arrow 向量攒批 → part-{p}-{a}.arrows（IPC stream：
 * Schema 帧 + N × RecordBatch + EOS）。
 *
 * <p>行→Arrow 转换成本在 executor（DSv2 行接口固有，D12）；攒批大小由 spec 的
 * maxRecordsPerBatch 控制。空分区不产文件（commit 返回 uri=null 的 PartMetadata，manifest
 * 跳过）。writer.abort() 删本 task 半成品；task retry 以 taskId 命名不互踩。
 *
 * <p>D27 scroll 翻页：IPC stream 文件格式不动，task 收尾时把每 batch 的
 * {@code (offset, length, rows)} 三元组写成边车 {@code {part}.bidx}（位置由输出 channel
 * 跟踪，对齐 padding 天然含在 length 内）——网关据此对 MinIO 做 Range GET 取单 message。
 * 边车是优化不是正确性依赖（缺席时网关回落整 part 顺序读）。
 *
 * <p>M1 注 → D33 落地：Spark 3.5 内嵌 Arrow 12.0.1 的 ArrowStreamWriter 支持压缩构造器
 * （CompressionCodec.Factory + CodecType），codec 实现在 arrow-compression（commons-compress
 * 后端，需随部署放入 $SPARK_HOME/jars）——spec.compression（none|zstd|lz4）即开即用。
 */
public final class FgArrowPartWriter implements DataWriter<InternalRow> {

  private static final Logger logger = LoggerFactory.getLogger(FgArrowPartWriter.class);
  private static final String TIMEZONE = "UTC";

  /**
   * D33：LZ4 写端 factory——arrow-compression 12 的 commons-compress 纯 Java LZ4 实测
   * 9.4s/MiB（不可用），换 aircompressor 后端的手写 frame 封装（executor 自带依赖）。
   */
  private static final org.apache.arrow.vector.compression.CompressionCodec.Factory FG_LZ4_FACTORY =
      new org.apache.arrow.vector.compression.CompressionCodec.Factory() {
        @Override
        public org.apache.arrow.vector.compression.CompressionCodec createCodec(
            CompressionUtil.CodecType codecType) {
          if (codecType == CompressionUtil.CodecType.LZ4_FRAME) {
            return new FgLz4FrameCodec();
          }
          return CommonsCompressionFactory.INSTANCE.createCodec(codecType);
        }

      };

  private final SpecOptions spec;
  private final int partitionId;
  private final long taskId;
  private final String partUri;

  private final ArrowWriter arrowWriter;
  private final VectorSchemaRoot root;
  private final ArrowStreamWriter streamWriter;
  private final PositionTrackingChannel channel;
  private final OutputStream out;
  private final FileSystem fs;

  private long rowCount = 0;
  private long pending = 0;
  /** D27：每 batch 的 (offset, length, rows) 三元组（边车 .bidx 的内容）。 */
  private final List<long[]> batchIndex = new ArrayList<>();

  public FgArrowPartWriter(
      StructType schema,
      SpecOptions spec,
      SerializableConfiguration hadoopConf,
      int partitionId,
      long taskId) {
    this.spec = spec;
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.partUri =
        spec.objectUri() + String.format("part-%d-%d.arrows", partitionId, taskId);
    ArrowWriter aw = null;
    ArrowStreamWriter sw = null;
    OutputStream o = null;
    FileSystem f = null;
    PositionTrackingChannel ch = null;
    try {
      Path path = new Path(URI.create(partUri));
      f = path.getFileSystem(hadoopConf.value());
      o = f.create(path, true);
      aw = ArrowWriter.create(schema, TIMEZONE, true);
      VectorSchemaRoot r = aw.root();
      ch = new PositionTrackingChannel(Channels.newChannel(o));
      // D33：IPC body 压缩（Arrow BodyCompression——Spark 3.5 内嵌 Arrow 12.0.1 的
      // ArrowStreamWriter 自带压缩构造器，codec 实现 arrow-compression[commons-compress 后端]
      // 需随部署放入 $SPARK_HOME/jars）。压缩按 encapsulated message 级生效，bidx 的
      // (offset,length) 跟踪的是压缩后字节——Range GET/miniStream 重组不受影响，读端
      // （网关/驱动/pyarrow）透明解压。
      switch (spec.compression()) {
        case "zstd" ->
            sw = new ArrowStreamWriter(
                r, null, ch, IpcOption.DEFAULT,
                CommonsCompressionFactory.INSTANCE, CompressionUtil.CodecType.ZSTD);
        case "lz4" ->
            sw = new ArrowStreamWriter(
                r, null, ch, IpcOption.DEFAULT, FG_LZ4_FACTORY, CompressionUtil.CodecType.LZ4_FRAME);
        default -> sw = new ArrowStreamWriter(r, null, ch);
      }
      sw.start();
    } catch (Exception e) {
      closeQuietly(sw, o, aw == null ? null : aw.root());
      throw new RuntimeException("Failed to open part writer for " + partUri, e);
    }
    this.arrowWriter = aw;
    this.root = aw.root();
    this.streamWriter = sw;
    this.channel = ch;
    this.out = o;
    this.fs = f;
    if (!spec.compression().equals("none")) {
      logger.info("Part compression enabled: codec={} part={}", spec.compression(), partUri);
    }
  }

  @Override
  public void write(InternalRow record) throws IOException {
    try {
      arrowWriter.write(record);
    } catch (RuntimeException e) {
      throw new IOException("Failed to encode row for " + partUri, e);
    }
    rowCount++;
    pending++;
    if (pending == spec.maxRecordsPerBatch()) {
      flushBatch();
    }
  }

  private void flushBatch() throws IOException {
    long rowsInBatch = pending;
    long start = channel.position();
    try {
      arrowWriter.finish();
      streamWriter.writeBatch();
    } catch (Exception e) {
      throw new IOException("Failed to flush batch for " + partUri, e);
    } finally {
      arrowWriter.reset();
      pending = 0;
    }
    // encapsulated message（continuation + metadata + 对齐 + body）自包含，
    // [start, end) 区间即该 batch 的完整字节——网关可独立 Range GET 读取
    batchIndex.add(new long[] {start, channel.position() - start, rowsInBatch});
  }

  @Override
  public PartMetadata commit() throws IOException {
    if (rowCount == 0) {
      // 空分区不产文件（manifest 跳过）
      closeStreams();
      deletePart();
      return new PartMetadata(null, partitionId, 0, 0, (int) taskId);
    }
    try {
      if (pending > 0) {
        flushBatch();
      }
      closeStreams(); // streamWriter.close() writes EOS
      writeBatchIndex();
      long bytes = fs.getFileStatus(new Path(URI.create(partUri))).getLen();
      logger.info("Part committed: {} rows={} bytes={} batches={}",
          partUri, rowCount, bytes, batchIndex.size());
      return new PartMetadata(partUri, partitionId, rowCount, bytes, (int) taskId);
    } catch (Exception e) {
      abort();
      throw new IOException("Failed to commit " + partUri, e);
    }
  }

  /** D27：边车 {part}.bidx——紧凑 JSON 数组 [[offset, length, rows], ...]。 */
  private void writeBatchIndex() throws IOException {
    StringBuilder sb = new StringBuilder("{\"batches\":[");
    for (int i = 0; i < batchIndex.size(); i++) {
      long[] t = batchIndex.get(i);
      if (i > 0) {
        sb.append(',');
      }
      sb.append('[').append(t[0]).append(',').append(t[1]).append(',').append(t[2]).append(']');
    }
    sb.append("]}");
    Path path = new Path(URI.create(partUri + FgResultSinks.BATCH_INDEX_SUFFIX));
    try (OutputStream out = fs.create(path, true)) {
      out.write(sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
  }

  @Override
  public void abort() throws IOException {
    closeStreams();
    deletePart();
  }

  @Override
  public void close() {
    // commit/abort 已完成资源清理；幂等兜底
    closeStreams();
  }

  private void closeStreams() {
    closeQuietly(streamWriter, out, root);
  }

  private void deletePart() {
    try {
      Path path = new Path(URI.create(partUri));
      if (fs != null && fs.exists(path)) {
        fs.delete(path, false);
      }
      Path bidx = new Path(URI.create(partUri + FgResultSinks.BATCH_INDEX_SUFFIX));
      if (fs != null && fs.exists(bidx)) {
        fs.delete(bidx, false);
      }
    } catch (Exception e) {
      logger.warn("Part cleanup failed for {}: {}", partUri, e.toString());
    }
  }

  /** 位置跟踪输出 channel（D27）：ArrowStreamWriter 写入位置的可靠来源。 */
  static final class PositionTrackingChannel implements WritableByteChannel {

    private final WritableByteChannel delegate;
    private long position;

    PositionTrackingChannel(WritableByteChannel delegate) {
      this.delegate = delegate;
    }

    long position() {
      return position;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      int n = delegate.write(src);
      position += n;
      return n;
    }

    @Override
    public boolean isOpen() {
      return delegate.isOpen();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  private static void closeQuietly(
      ArrowStreamWriter streamWriter, OutputStream out, VectorSchemaRoot root) {
    try {
      if (streamWriter != null) {
        streamWriter.close();
      }
    } catch (Exception ignored) {
    }
    try {
      if (out != null) {
        out.close();
      }
    } catch (Exception ignored) {
    }
    try {
      if (root != null) {
        root.close();
      }
    } catch (Exception ignored) {
    }
  }
}
