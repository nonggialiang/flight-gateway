package org.fg.orchestrator;

import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.fg.common.sql.StatementClassifier.Kind;

/**
 * 非 analyzable 命令的静态宣告 schema（design D17/D18）：Dremio 式客户端兼容要求一切语句
 * 的 GetFlightInfo/PollFlightInfo 宣告非空 schema。
 *
 * <ul>
 *   <li>SET/RESET → [key, value UTF8]；</li>
 *   <li>DML → [num_affected_rows BIGINT]（终态以引擎实际返回的 schema CAS 覆盖——Spark
 *       INSERT 可能返回 [num_affected_rows, num_inserted_rows] 两列，poll 客户端自洽）；</li>
 *   <li>DDL/USE → [ok BOOLEAN]；</li>
 *   <li>analyzable kind（QUERY/SHOW/DESCRIBE/EXPLAIN）→ null（inline AnalyzePlan 取真实
 *       schema，不走静态宣告）。</li>
 * </ul>
 */
public final class CommandSchemas {

  private CommandSchemas() {}

  /** 静态宣告 schema；analyzable kind 返回 null。 */
  public static Schema staticSchema(Kind kind) {
    switch (kind) {
      case SET:
      case RESET:
        return new Schema(List.of(
            new Field("key", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("value", FieldType.nullable(new ArrowType.Utf8()), null)));
      case DML:
        return new Schema(List.of(
            new Field("num_affected_rows", FieldType.nullable(new ArrowType.Int(64, true)), null)));
      case DDL:
      case USE:
        return new Schema(List.of(
            new Field("ok", FieldType.nullable(new ArrowType.Bool()), null)));
      default:
        return null; // QUERY/SHOW/DESCRIBE/EXPLAIN：analyzable，inline AnalyzePlan
    }
  }
}
