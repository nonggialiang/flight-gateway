package org.fg.spark.sink;

import java.util.Map;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/**
 * fg-result 写构建器。全表 overwrite（mode=Overwrite 无过滤条件）走 SupportsTruncate 路径
 * （V2Writes 规则匹配 builder 的 truncate()）。
 */
public final class FgWriteBuilder implements WriteBuilder, SupportsTruncate {

  private final StructType schema;
  private final Map<String, String> options;

  public FgWriteBuilder(StructType schema, Map<String, String> options) {
    this.schema = schema;
    this.options = options;
  }

  @Override
  public WriteBuilder truncate() {
    return this;
  }

  @Override
  public BatchWrite buildForBatch() {
    return new FgBatchWrite(schema, options);
  }
}
