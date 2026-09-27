package org.fg.common.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * JDBC pattern 匹配（D21 元数据过滤）：null/空/全匹配/单字符/中文/转义/大小写（JDBC 语义
 * 大小写敏感存储、过滤大小写敏感）表格驱动，(pattern, value, expected) 三元组显式给期望。
 */
class TestSqlPatternMatcher {

  static Stream<Arguments> cases() {
    return Stream.of(
        // null pattern = 不过滤（恒 true）
        Arguments.of(null, "anything", true),
        Arguments.of(null, "", true),
        Arguments.of(null, null, true),
        // 全匹配
        Arguments.of("%", "anything", true),
        Arguments.of("%", "", true),
        Arguments.of("%", "表名", true),
        Arguments.of("%%", "", true),
        Arguments.of("%", "x", true),
        // 字面精确
        Arguments.of("default", "default", true),
        Arguments.of("fg_e2e_t", "fg_e2e_t", true),
        // 前缀/中缀/后缀
        Arguments.of("def%", "default", true),
        Arguments.of("def%", "definitions", true),
        Arguments.of("%meta%", "fg_e2e_meta_t", true),
        Arguments.of("%meta%", "metadata_v2", true),
        Arguments.of("%_t", "fg_e2e_t", true),
        Arguments.of("%_t", "ab_t", true),
        // 单字符
        Arguments.of("def_ult", "default", true),
        Arguments.of("_______", "default", true), // 7 字符
        Arguments.of("______", "default", false), // 6 字符 vs 7
        Arguments.of("_%", "x", true),
        // 不匹配：大小写敏感（JDBC 语义）
        Arguments.of("def%", "DEFault", false),
        Arguments.of("default", "DEFAULT", false),
        // 不匹配：其它
        Arguments.of("def%", "dxfault", false),
        Arguments.of("default", "defaults", false), // 全锚定：非子串
        Arguments.of("defaults", "default", false),
        Arguments.of("def_ult", "deful", false), // _ 恰一字符
        Arguments.of("def_ult", "defullt", false),
        Arguments.of("a", null, false), // null value 视为空串
        // 空 pattern 仅匹配空串（与 null 语义不同）
        Arguments.of("", "", true),
        Arguments.of("", "default", false),
        // 转义（\% 与 \_ 字面）
        Arguments.of("100\\%", "100%", true),
        Arguments.of("100\\%", "100x", false),
        Arguments.of("\\_t", "_t", true),
        Arguments.of("\\_t", "at", false),
        Arguments.of("fg\\_e2e\\_t", "fg_e2e_t", true),
        Arguments.of("fg\\_e2e\\_t", "fgXe2e_t", false),
        // 正则元字符按字面（Pattern.quote 分段拼接）
        Arguments.of("a.b+c(d)", "a.b+c(d)", true),
        Arguments.of("a.b+c(d)", "axbyczd", false),
        Arguments.of("[abc]", "[abc]", true),
        Arguments.of("[abc]", "a", false),
        // 反斜杠字面（非 %/_ 前缀按字面 \x，JDBC 惯例）
        Arguments.of("a\\b", "a\\b", true),
        Arguments.of("a\\", "a\\", true),
        // 尾悬反斜杠按字面
        Arguments.of("a\\", "a", false));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void tableDriven(String pattern, String value, boolean expected) {
    assertThat(SqlPatternMatcher.matches(pattern, value))
        .as("pattern='%s' value='%s'", pattern, value)
        .isEqualTo(expected);
  }

  @Test
  void chineseIdentifiers() {
    assertThat(SqlPatternMatcher.matches("%库%", "默认库")).isTrue();
    assertThat(SqlPatternMatcher.matches("默认_", "默认库")).isTrue();
    assertThat(SqlPatternMatcher.matches("默认", "默认库")).isFalse();
  }
}
