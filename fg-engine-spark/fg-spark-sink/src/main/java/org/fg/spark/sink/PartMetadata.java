package org.fg.spark.sink;

import java.io.Serializable;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * 单个 part 的元数据（= WriterCommitMessage，对标 Iceberg DataFile）。
 *
 * <p>uri 为 null 表示空分区（不产文件，manifest 跳过——避免 0 行 part 污染 endpoint 列表）。
 */
public final class PartMetadata implements WriterCommitMessage, Serializable {

  private static final long serialVersionUID = 1L;

  private final String uri;
  private final int partitionIndex;
  private final long recordCount;
  private final long bytes;
  private final int attempt;

  public PartMetadata(String uri, int partitionIndex, long recordCount, long bytes, int attempt) {
    this.uri = uri;
    this.partitionIndex = partitionIndex;
    this.recordCount = recordCount;
    this.bytes = bytes;
    this.attempt = attempt;
  }

  public String uri() {
    return uri;
  }

  public int partitionIndex() {
    return partitionIndex;
  }

  public long recordCount() {
    return recordCount;
  }

  public long bytes() {
    return bytes;
  }

  public int attempt() {
    return attempt;
  }

  @Override
  public String toString() {
    return "PartMetadata{uri="
        + uri
        + ", index="
        + partitionIndex
        + ", rows="
        + recordCount
        + ", bytes="
        + bytes
        + ", attempt="
        + attempt
        + "}";
  }
}
