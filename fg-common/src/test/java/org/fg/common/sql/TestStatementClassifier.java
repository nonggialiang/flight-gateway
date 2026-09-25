package org.fg.common.sql;

import static org.assertj.core.api.Assertions.assertThat;

import org.fg.common.sql.StatementClassifier.Kind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** 语句分类器（design D17）：表格驱动覆盖各 kind/注释/分号/二词前缀/小写/未知→QUERY。 */
class TestStatementClassifier {

  @ParameterizedTest
  @CsvSource({
    "SELECT 1, QUERY",
    "WITH t AS (SELECT 1) SELECT * FROM t, QUERY",
    "with t as (select 1) select * from t, QUERY",
    "SET spark.sql.adaptive.enabled=true, SET",
    "set k = v, SET",
    "RESET spark.sql.adaptive.enabled, RESET",
    "RESET ALL, RESET",
    "SHOW DATABASES, SHOW",
    "SHOW TABLES IN db, SHOW",
    "DESCRIBE TABLE t, DESCRIBE",
    "DESC TABLE t, DESCRIBE",
    "desc t, DESCRIBE",
    "EXPLAIN SELECT 1, EXPLAIN",
    "USE db, USE",
    "INSERT INTO t VALUES (1), DML",
    "INSERT OVERWRITE TABLE t SELECT 1, DML",
    "UPDATE t SET a = 1, DML",
    "DELETE FROM t, DML",
    "MERGE INTO t USING s ON 1 = 1, DML",
    "CREATE TABLE t (id BIGINT), DDL",
    "CREATE OR REPLACE VIEW v AS SELECT 1, DDL",
    "DROP TABLE t, DDL",
    "ALTER TABLE t ADD COLUMN c INT, DDL",
    "TRUNCATE TABLE t, DDL"
  })
  void classifyKinds(String sql, Kind expected) {
    assertThat(StatementClassifier.classify(sql)).isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "  ",
      "",
      "-- only a comment\n",
      ";;",
      "CREATEFUNCTIONLIKE thing",
      "SETTINGS foo",
      "nonexistent keyword"
  })
  void unknownDefaultsToQuery(String sql) {
    assertThat(StatementClassifier.classify(sql)).isEqualTo(Kind.QUERY);
  }

  @Test
  void commentsAndLeadingSemicolonsStripped() {
    assertThat(StatementClassifier.classify("-- lead comment\nSET k = v;")).isEqualTo(Kind.SET);
    assertThat(StatementClassifier.classify("/* block */ SHOW DATABASES")).isEqualTo(Kind.SHOW);
    assertThat(StatementClassifier.classify(";;\nDESCRIBE TABLE t")).isEqualTo(Kind.DESCRIBE);
    assertThat(StatementClassifier.classify("/* multi\nline */ USE db"))
        .isEqualTo(Kind.USE);
  }

  @Test
  void commentMarkersInsideStringLiteralsAreNotComments() {
    // 字符串里的 "--" 不是注释：首关键字照样可判
    assertThat(StatementClassifier.classify("SET k = 'val--not-comment'")).isEqualTo(Kind.SET);
    assertThat(StatementClassifier.classify("SELECT 'a--b'")).isEqualTo(Kind.QUERY);
    assertThat(StatementClassifier.classify("SELECT '/* not comment */' FROM t"))
        .isEqualTo(Kind.QUERY);
  }

  @Test
  void analyzableKinds() {
    for (Kind kind : Kind.values()) {
      boolean expected =
          kind == Kind.QUERY || kind == Kind.SHOW || kind == Kind.DESCRIBE || kind == Kind.EXPLAIN;
      assertThat(StatementClassifier.analyzable(kind))
          .as("analyzable(%s)", kind)
          .isEqualTo(expected);
    }
  }
}
