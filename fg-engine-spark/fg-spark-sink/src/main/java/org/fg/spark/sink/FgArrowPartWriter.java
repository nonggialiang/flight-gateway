package org.fg.spark.sink;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
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
 * <p>M1 注：Spark 3.5 发行版内嵌 Arrow 12 的 ArrowStreamWriter 不支持 IPC body 压缩，
 * zstd 标志暂不生效（保留 spec 字段，升级引擎 Arrow 后启用）。
 */
public final class FgArrowPartWriter implements DataWriter<InternalRow> {

  private static final Logger logger = LoggerFactory.getLogger(FgArrowPartWriter.class);
  private static final String TIMEZONE = "UTC";

  private final SpecOptions spec;
  private final int partitionId;
  private final long taskId;
  private final String partUri;

  private final ArrowWriter arrowWriter;
  private final VectorSchemaRoot root;
  private final ArrowStreamWriter streamWriter;
  private final OutputStream out;
  private final FileSystem fs;

  private long rowCount = 0;
  private long pending = 0;

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
    try {
      Path path = new Path(URI.create(partUri));
      f = path.getFileSystem(hadoopConf.value());
      o = f.create(path, true);
      aw = ArrowWriter.create(schema, TIMEZONE, true);
      VectorSchemaRoot r = aw.root();
      sw = new ArrowStreamWriter(r, null, Channels.newChannel(o));
      sw.start();
    } catch (Exception e) {
      closeQuietly(sw, o, aw == null ? null : aw.root());
      throw new RuntimeException("Failed to open part writer for " + partUri, e);
    }
    this.arrowWriter = aw;
    this.root = aw.root();
    this.streamWriter = sw;
    this.out = o;
    this.fs = f;
    if (spec.zstdCompression()) {
      logger.warn("zstdCompression requested but engine Arrow (12.x) cannot compress IPC bodies; writing uncompressed part: {}", partUri);
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
    try {
      arrowWriter.finish();
      streamWriter.writeBatch();
    } catch (Exception e) {
      throw new IOException("Failed to flush batch for " + partUri, e);
    } finally {
      arrowWriter.reset();
      pending = 0;
    }
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
      long bytes = fs.getFileStatus(new Path(URI.create(partUri))).getLen();
      logger.info("Part committed: {} rows={} bytes={}", partUri, rowCount, bytes);
      return new PartMetadata(partUri, partitionId, rowCount, bytes, (int) taskId);
    } catch (Exception e) {
      abort();
      throw new IOException("Failed to commit " + partUri, e);
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
    } catch (Exception e) {
      logger.warn("Part cleanup failed for {}: {}", partUri, e.toString());
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
