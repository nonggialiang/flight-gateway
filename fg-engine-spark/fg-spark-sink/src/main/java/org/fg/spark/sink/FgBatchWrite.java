package org.fg.spark.sink;

import java.util.Map;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

/**
 * fg-result 批写（design §4.4.2，对标 Iceberg SparkWrite）：commit 聚合 manifest；abort 按
 * 精确清单删 part。driver 不在数据路径（连 Arrow 编码都在 executor）。
 */
public final class FgBatchWrite implements BatchWrite {

  private final StructType schema;
  private final SpecOptions spec;

  public FgBatchWrite(StructType schema, Map<String, String> options) {
    this.schema = schema;
    this.spec = SpecOptions.parse(options, options.get("path"));
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(
      org.apache.spark.sql.connector.write.PhysicalWriteInfo info) {
    SerializableConfiguration hadoopConf =
        new SerializableConfiguration(
            org.apache.spark.SparkContext.getOrCreate().hadoopConfiguration());
    return new FgArrowWriterFactory(schema, spec, hadoopConf);
  }

  @Override
  public boolean useCommitCoordinator() {
    return false;
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    ManifestWriter.write(spec, schema, cast(messages));
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    ManifestWriter.abort(spec, cast(messages));
  }

  private static PartMetadata[] cast(WriterCommitMessage[] messages) {
    PartMetadata[] parts = new PartMetadata[messages.length];
    for (int i = 0; i < messages.length; i++) {
      parts[i] = (PartMetadata) messages[i];
    }
    return parts;
  }
}
