package org.fg.spark.sink;

import java.io.Serializable;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

/** 每task一个 FgArrowPartWriter 的工厂（executor 侧反序列化）。 */
public final class FgArrowWriterFactory implements DataWriterFactory, Serializable {

  private static final long serialVersionUID = 1L;

  private final StructType schema;
  private final SpecOptions spec;
  private final SerializableConfiguration hadoopConf;

  public FgArrowWriterFactory(
      StructType schema, SpecOptions spec, SerializableConfiguration hadoopConf) {
    this.schema = schema;
    this.spec = spec;
    this.hadoopConf = hadoopConf;
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    return new FgArrowPartWriter(schema, spec, hadoopConf, partitionId, taskId);
  }
}
