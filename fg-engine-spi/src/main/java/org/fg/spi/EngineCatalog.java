package org.fg.spi;

import java.util.List;

/**
 * 引擎元数据目录（design §4.8，v1 完整实现落 M2）。pattern 过滤在 gateway 侧完成（JDBC %/_
 * 翻译），引擎侧不做过滤。
 */
public interface EngineCatalog {

  List<EngineDatabase> listDatabases();

  List<EngineTable> listTables(String database);

  List<EngineColumn> listColumns(String database, String table);

  record EngineDatabase(String name, String description) {}

  record EngineTable(
      String database, String name, String type, boolean isTemporary) {}

  record EngineColumn(
      String name, String typeName, boolean nullable, String comment) {}
}
