package org.fg.spark.client

import org.fg.common.config.GatewayConfig
import org.fg.spi.{CommandOutcome, EngineCatalog, EngineSession}

import scala.collection.JavaConverters._

/**
 * Spark 引擎目录实现（design §4.8/D21，M2）：catalog 完全在引擎侧——四条 SHOW/DESCRIBE
 * 经命令管道（executeCommand 内联同步）取数，零新引擎协议。
 *
 * <ul>
 *   <li>三层名映射：Flight SQL (catalog, db_schema, table) = Spark (catalog, database,
 *       table)——`SHOW SCHEMAS IN \`cat\`` / `SHOW TABLES IN \`cat\`.\`db\`` /
 *       `DESCRIBE TABLE \`cat\`.\`db\`.\`t\``（拼入 SQL 的名字一律反引号包裹，内嵌反引号双写）；</li>
 *   <li>同步等待界限：executeCommand future `.get(fg.query.prepare.timeout)`（沿用
 *       analyzeSchema 的有界阻塞先例），超时/失败上抛 IllegalStateException；</li>
 *   <li>table_type 判定：SHOW TABLES 的 isTemporary=true → VIEW，否则 TABLE；</li>
 *   <li>DESCRIBE TABLE 分节行（空 col_name / `#` 前缀的 Partition Information 头）跳过。</li>
 * </ul>
 */
final class SparkEngineCatalog(
    config: GatewayConfig,
    client: SparkEngineClient,
    session: EngineSession)
    extends EngineCatalog {

  private val s = session.asInstanceOf[SparkEngineSession]
  private val timeoutMs = config.getDurationMs("fg.query.prepare.timeout")

  override def listCatalogs(): java.util.List[String] =
    run("SHOW CATALOGS").asScala
      .flatMap(row => Option(str(row, "catalog")))
      .distinct
      .toList
      .asJava

  override def listDatabases(catalog: String): java.util.List[EngineCatalog.EngineDatabase] =
    run(s"SHOW SCHEMAS IN ${quote(catalog)}").asScala
      .map(row => new EngineCatalog.EngineDatabase(str(row, "namespace"), null))
      .toList
      .asJava

  override def listTables(
      catalog: String,
      database: String): java.util.List[EngineCatalog.EngineTable] =
    run(s"SHOW TABLES IN ${quote(catalog)}.${quote(database)}").asScala
      .map { row =>
        val temporary = row.get("isTemporary") match {
          case b: java.lang.Boolean => b.booleanValue()
          case _ => false
        }
        new EngineCatalog.EngineTable(
          catalog, database, str(row, "tableName"),
          if (temporary) "VIEW" else "TABLE", temporary)
      }
      .toList
      .asJava

  override def listColumns(
      catalog: String,
      database: String,
      table: String): java.util.List[EngineCatalog.EngineColumn] =
    run(s"DESCRIBE TABLE ${quote(catalog)}.${quote(database)}.${quote(table)}").asScala
      .filter { row =>
        // 分节行（分区信息头等）：空 col_name / '#' 前缀说明行
        val name = str(row, "col_name")
        name != null && !name.trim.isEmpty && !name.trim.startsWith("#")
      }
      .map { row =>
        // DESCRIBE 不给 nullability（listColumns 语义兜底 nullable=true）
        new EngineCatalog.EngineColumn(
          str(row, "col_name").trim, str(row, "data_type"), true, str(row, "comment"))
      }
      .toList
      .asJava

  // ------------------------------------------------------------- 命令管道取数

  /** SHOW/DESCRIBE → executeCommand（持流至终态）→ IPC bytes 解行为行 Map（列名→值）。 */
  private def run(sql: String): java.util.List[java.util.Map[String, AnyRef]] = {
    val future = client.executeCommand(s, sql, _ => ())
    val outcome =
      try future.get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
      catch {
        case _: java.util.concurrent.TimeoutException =>
          throw new IllegalStateException(
            s"metadata listing timed out after ${timeoutMs}ms: $sql")
        case e: java.util.concurrent.ExecutionException =>
          throw new IllegalStateException(
            s"metadata listing failed: ${Option(e.getCause).getOrElse(e)}")
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          throw new IllegalStateException(s"metadata listing interrupted: $sql")
      }
    outcome.status() match {
      case CommandOutcome.OutcomeStatus.COMPLETED => // ok
      case _ =>
        throw new IllegalStateException(
          s"metadata listing failed: ${Option(outcome.error()).getOrElse(outcome.status().toString)}")
    }

    val rows = new java.util.ArrayList[java.util.Map[String, AnyRef]]()
    val allocator = new org.apache.arrow.memory.RootAllocator(Long.MaxValue)
    try {
      val reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(
        new java.io.ByteArrayInputStream(outcome.resultIpcBytes()), allocator)
      try {
        val root = reader.getVectorSchemaRoot
        while (reader.loadNextBatch()) {
          val fieldNames = root.getSchema.getFields.asScala.map(_.getName).toIndexedSeq
          for (i <- 0 until root.getRowCount) {
            val row = new java.util.LinkedHashMap[String, AnyRef]()
            for (j <- fieldNames.indices) {
              row.put(fieldNames(j), valueOf(root.getVector(j), i))
            }
            rows.add(row)
          }
        }
      } finally reader.close()
    } finally allocator.close()
    rows
  }

  /** 按向量类型取值（SHOW/DESCRIBE 面：string/bool/long/int；其余 toString 兜底）。 */
  private def valueOf(vector: org.apache.arrow.vector.ValueVector, index: Int): AnyRef =
    vector match {
      case v: org.apache.arrow.vector.VarCharVector =>
        if (v.isNull(index)) null
        else new String(v.get(index), java.nio.charset.StandardCharsets.UTF_8)
      case v: org.apache.arrow.vector.BitVector =>
        if (v.isNull(index)) null else java.lang.Boolean.valueOf(v.get(index) == 1)
      case v: org.apache.arrow.vector.BigIntVector =>
        if (v.isNull(index)) null else java.lang.Long.valueOf(v.get(index))
      case v: org.apache.arrow.vector.IntVector =>
        if (v.isNull(index)) null else java.lang.Integer.valueOf(v.get(index))
      case other =>
        val o = other.getObject(index)
        if (o == null) null else String.valueOf(o)
    }

  private def str(row: java.util.Map[String, AnyRef], key: String): String =
    row.get(key).asInstanceOf[String]

  /** 标识符反引号包裹（内嵌反引号双写转义，Spark SQL 语法）。 */
  private def quote(name: String): String = s"`${name.replace("`", "``")}`"
}
