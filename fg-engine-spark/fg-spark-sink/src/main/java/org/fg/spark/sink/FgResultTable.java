package org.fg.spark.sink;

import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.SupportsOverwrite;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/**
 * fg-result 表（SupportsWrite + Overwrite/Truncate）。每查询前缀唯一，overwrite 语义即
 * "全新写入"（不存在部分覆盖场景）。
 */
public final class FgResultTable implements Table, SupportsWrite, SupportsOverwrite, SupportsTruncate {

  private static final StructType PLACEHOLDER = new StructType();

  private final Map<String, String> options;

  public FgResultTable(Map<String, String> options) {
    this.options = options;
  }

  @Override
  public String name() {
    return FgResultDataSource.SHORT_NAME;
  }

  /** 占位 schema（write-only；真实查询 schema 经 LogicalWriteInfo 传入 write builder）。 */
  @Override
  public StructType schema() {
    return PLACEHOLDER;
  }

  @Override
  public Transform[] partitioning() {
    return new Transform[0];
  }

  @Override
  public Map<String, String> properties() {
    return options;
  }

  @Override
  public Set<TableCapability> capabilities() {
    // mode(overwrite) 全表覆写要求 TRUNCATE/OVERWRITE_BY_FILTER 能力（TableCapabilityCheck）
    return java.util.EnumSet.of(
        TableCapability.BATCH_WRITE,
        TableCapability.ACCEPT_ANY_SCHEMA,
        TableCapability.TRUNCATE,
        TableCapability.OVERWRITE_BY_FILTER);
  }

  @Override
  public boolean canOverwrite(org.apache.spark.sql.sources.Filter[] filters) {
    return filters == null || filters.length == 0;
  }

  @Override
  public WriteBuilder overwrite(org.apache.spark.sql.sources.Filter[] filters) {
    return new FgWriteBuilder(PLACEHOLDER, options);
  }

  @Override
  public WriteBuilder truncate() {
    return new FgWriteBuilder(PLACEHOLDER, options);
  }

  @Override
  public WriteBuilder newWriteBuilder(
      org.apache.spark.sql.connector.write.LogicalWriteInfo info) {
    // 查询 schema 在此进入 sink（write-only 表的 schema 真相源）
    return new FgWriteBuilder(info.schema(), info.options());
  }
}
