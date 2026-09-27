package org.fg.spi;

import java.util.List;

/**
 * 引擎元数据目录（design §4.8/D21，M2）。pattern 过滤在 gateway 侧完成（JDBC %/_
 * 翻译，{@code SqlPatternMatcher}），引擎侧不做过滤。
 *
 * <p>三层名映射：Flight SQL (catalog, db_schema, table) = Spark (catalog, database, table)。
 * catalog 维度动态来自引擎（Spark 3.3+ SHOW CATALOGS，SPARK-35973——目录插件注册的
 * Iceberg/Delta 等自动出现）；实现经命令管道（SHOW/DESCRIBE → executeCommand 内联同步）
 * 取数，零新引擎协议。
 */
public interface EngineCatalog {

  /** 引擎实有 catalog 名（Spark：SHOW CATALOGS；含会话 catalog 真名 spark_catalog）。 */
  List<String> listCatalogs();

  List<EngineDatabase> listDatabases(String catalog);

  List<EngineTable> listTables(String catalog, String database);

  List<EngineColumn> listColumns(String catalog, String database, String table);

  record EngineDatabase(String name, String description) {}

  record EngineTable(
      String catalog, String database, String name, String type, boolean isTemporary) {}

  record EngineColumn(
      String name, String typeName, boolean nullable, String comment) {}
}
