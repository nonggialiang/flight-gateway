package org.fg.common.sql;

import java.util.regex.Pattern;

/**
 * JDBC 风格 pattern 匹配（Flight SQL 元数据过滤，design §4.8/D21）：Flight SQL 的
 * db_schema_filter_pattern / table_name_filter_pattern 沿用 JDBC 语义——{@code %} 匹配
 * 任意 0+ 字符、{@code _} 匹配任意单字符、其余字面；null pattern = 不过滤。
 *
 * <ul>
 *   <li>转义：反斜杠前缀的 {@code \%} / {@code \_} 表示字面 % / _（JDBC escape 语法）；
 *   <li>大小写敏感（JDBC 语义：存储大小写敏感、过滤亦然）；
 *   <li>全锚定（pattern 覆盖整个标识符，非子串搜索）；
 *   <li>空 pattern 仅匹配空串（与 JDBC 一致；null 与 "" 语义不同——网关入口对 catalog
 *       字段另有宽容处理，见 producer）。
 * </ul>
 */
public final class SqlPatternMatcher {

  private SqlPatternMatcher() {}

  /** null pattern → 恒 true（不过滤）；value 为 null 视为空串（不匹配非空 pattern）。 */
  public static boolean matches(String pattern, String value) {
    if (pattern == null) {
      return true;
    }
    String target = value == null ? "" : value;
    return compile(pattern).matcher(target).matches();
  }

  /**
   * pattern → 全锚定正则：分段拼接（Pattern.quote 字面段 + 通配段），避免逐字符转义的
   * 遗漏。{@code %}→{@code .*}、{@code _}→{@code .}、{@code \%}/{\@code \_}→字面。
   */
  private static Pattern compile(String pattern) {
    StringBuilder regex = new StringBuilder();
    StringBuilder literal = new StringBuilder();
    boolean escaped = false;
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (escaped) {
        // 仅 % 与 _ 可转义；其余 \x 按 JDBC 惯例视为字面 \x（保留反斜杠）
        if (c == '%' || c == '_') {
          literal.append(c);
        } else {
          literal.append('\\').append(c);
        }
        escaped = false;
        continue;
      }
      if (c == '\\') {
        escaped = true;
        continue;
      }
      if (c == '%' || c == '_') {
        if (literal.length() > 0) {
          regex.append(Pattern.quote(literal.toString()));
          literal.setLength(0);
        }
        regex.append(c == '%' ? ".*" : ".");
        continue;
      }
      literal.append(c);
    }
    if (escaped) { // 尾悬反斜杠：按字面处理
      literal.append('\\');
    }
    if (literal.length() > 0) {
      regex.append(Pattern.quote(literal.toString()));
    }
    return Pattern.compile(regex.toString());
  }
}
