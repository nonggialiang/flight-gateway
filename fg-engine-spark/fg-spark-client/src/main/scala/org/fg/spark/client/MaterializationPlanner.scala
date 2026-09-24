package org.fg.spark.client

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema => ArrowSchema}
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.TimeUnit
import org.apache.spark.connect.proto.{AnalyzePlanRequest, Command, DataType, ExecutePlanRequest, Plan, Relation, Repartition, SQL, UserContext, WriteOperation}
import org.fg.spi.MaterializationSpec

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Connect plan 组装与 schema 转换（design §4.4.3）。
 *
 * <p>与官方 Scala 客户端 `spark.sql(sql).write.format("fg-result").mode("overwrite").save(path)`
 * 生成的 plan 同构（Command.write_operation{input: SQL 关系}，PoC① 因此预验证通过）：
 * partitionsHint match：null=自然分区；1=Repartition(1, shuffle=false)（coalesce 语义）；
 * k=Repartition(k, shuffle=true)。
 */
object MaterializationPlanner {

  private val MAPPER = new ObjectMapper()

  def specJson(spec: MaterializationSpec): String = {
    val node = MAPPER.createObjectNode()
    node.put("objectUri", spec.objectUri())
    node.put("zstdCompression", spec.zstdCompression())
    node.put("maxRecordsPerBatchHint", spec.maxRecordsPerBatchHint())
    if (spec.partitionsHint() != null) {
      node.put("partitionsHint", spec.partitionsHint())
    }
    MAPPER.writeValueAsString(node)
  }

  /** executePlan 请求：WriteOperation（经 Command）+ REATTACHABLE（reattachable 主路径）。 */
  def executePlanRequest(
      user: String,
      sessionId: String,
      sqlText: String,
      spec: MaterializationSpec): ExecutePlanRequest = {
    val base = Relation.newBuilder().setSql(SQL.newBuilder().setQuery(sqlText)).build()
    val wrapped = if (spec.partitionsHint() == null) {
      base
    } else {
      val shuffle = spec.partitionsHint() > 1 // =1 走 coalesce 语义（shuffle=false）
      Relation.newBuilder()
        .setRepartition(
          Repartition.newBuilder()
            .setInput(base)
            .setNumPartitions(spec.partitionsHint())
            .setShuffle(shuffle))
        .build()
    }

    val write = WriteOperation.newBuilder()
      .setInput(wrapped)
      .setSource("fg-result")
      .setMode(WriteOperation.SaveMode.SAVE_MODE_OVERWRITE)
      .setPath(spec.objectUri())
      .putOptions("spec", specJson(spec))

    val plan = Plan.newBuilder()
      .setCommand(Command.newBuilder().setWriteOperation(write))
    ExecutePlanRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(UserContext.newBuilder().setUserId(user))
      .setClientType("fg-gateway")
      .setPlan(plan)
      .addRequestOptions(
        ExecutePlanRequest.RequestOption.newBuilder()
          .setReattachOptions(
            org.apache.spark.connect.proto.ReattachOptions.newBuilder()
              .setReattachable(true)))
      .build()
  }

  /** AnalyzePlan 请求：SQL 关系的 schema 分析。 */
  def analyzePlanRequest(user: String, sessionId: String, sqlText: String): AnalyzePlanRequest = {
    val queryPlan = Plan.newBuilder()
      .setRoot(Relation.newBuilder().setSql(SQL.newBuilder().setQuery(sqlText)))
      .build()
    AnalyzePlanRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(UserContext.newBuilder().setUserId(user))
      .setClientType("fg-gateway")
      .setSchema(AnalyzePlanRequest.Schema.newBuilder().setPlan(queryPlan))
      .build()
  }

  // ---------------------------------------------------------------- schema 转换

  /** proto DataType(Struct) → Arrow Schema。列名去重（H1：快返 schema 与 manifest 一致语义）。 */
  def toArrowSchema(struct: DataType.Struct): ArrowSchema = {
    val seen = mutable.Set[String]()
    val fields = new util.ArrayList[Field]()
    struct.getFieldsList.asScala.foreach { f =>
      var name = f.getName
      var i = 1
      while (seen.contains(name)) {
        name = f.getName + "_" + i
        i += 1
      }
      seen += name
      // 空值标志须透传 Spark Connect AnalyzePlan 的 nullable：sink 落盘 schema 带真实可空性，
      // 宣告 schema 与物化 schema 不一致会被严格客户端拒绝（ADBC 逐 endpoint 校验）
      val fieldType =
        if (f.getNullable) FieldType.nullable(toArrowType(f.getDataType))
        else FieldType.notNullable(toArrowType(f.getDataType))
      fields.add(new Field(name, fieldType, null))
    }
    new ArrowSchema(fields)
  }

  private[client] def toArrowType(t: DataType): ArrowType = {
    import DataType.KindCase
    t.getKindCase match {
      case KindCase.NULL => ArrowType.Null.INSTANCE
      case KindCase.BOOLEAN => ArrowType.Bool.INSTANCE
      case KindCase.BYTE => new ArrowType.Int(8, true)
      case KindCase.SHORT => new ArrowType.Int(16, true)
      case KindCase.INTEGER => new ArrowType.Int(32, true)
      case KindCase.LONG => new ArrowType.Int(64, true)
      case KindCase.FLOAT => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case KindCase.DOUBLE => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case KindCase.STRING => new ArrowType.Utf8()
      case KindCase.CHAR => new ArrowType.Utf8()
      case KindCase.VAR_CHAR => new ArrowType.Utf8()
      case KindCase.BINARY => new ArrowType.Binary()
      case KindCase.DATE => new ArrowType.Date(DateUnit.DAY)
      case KindCase.TIMESTAMP => new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")
      case KindCase.TIMESTAMP_NTZ => new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
      case KindCase.DECIMAL =>
        new ArrowType.Decimal(t.getDecimal.getPrecision, t.getDecimal.getScale, 128)
      case _ => new ArrowType.Utf8() // array/map/struct/udt M1 退化
    }
  }
}
