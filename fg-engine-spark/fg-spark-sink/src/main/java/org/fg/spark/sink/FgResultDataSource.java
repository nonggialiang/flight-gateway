package org.fg.spark.sink;

import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * fg-result DataSource V2 入口（design §4.4.2，注册名 "fg-result"）。
 *
 * <p>物化参数经 options 传递（"spec" = MaterializationSpec JSON）；写入路径 = WriteOperation 的
 * {@code .format("fg-result").option("spec", ...).mode("overwrite").save(objectUri)}。
 */
public final class FgResultDataSource implements TableProvider, DataSourceRegister {

  public static final String SHORT_NAME = "fg-result";
  public static final String SPEC_OPTION = "spec";

  @Override
  public String shortName() {
    return SHORT_NAME;
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    // write-only：DataFrameWriter.save 需要 non-null schema 建表；真实查询 schema 经
    // LogicalWriteInfo.schema() 在 newWriteBuilder 处传入（capabilities 含 ACCEPT_ANY_SCHEMA）
    return new StructType();
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return new FgResultTable(properties);
  }
}

