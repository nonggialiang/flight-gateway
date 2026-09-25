package org.fg.common.sql;

import java.util.Locale;
import java.util.Set;

/**
 * 语句分类器（design D17）：非 SELECT 语句支持的地基——分类必须先于 AnalyzePlan
 * （SET/DDL 不可分析，AnalyzePlan 会直接抛错）。
 *
 * <ul>
 *   <li>启发式：剥注释/前导分号，首关键字 + 少量二词前缀（INSERT OVERWRITE、
 *       CREATE OR REPLACE）判定；未知一律 QUERY（宁可走查询路径也不误杀）；</li>
 *   <li>analyzable(kind)：QUERY|SHOW|DESCRIBE|EXPLAIN——引擎 AnalyzePlan 能给出
 *       result schema 的 kind（分类后 inline 取真实 schema）；其余（SET/RESET/USE/DML/DDL）
 *       用 {@code CommandSchemas} 静态宣告 schema；</li>
 *   <li>限制（记录为分类器契约）：只看首关键字，多语句 SQL 不识别（首语句决定分类）。
 * </ul>
 */
public final class StatementClassifier {

  /** 语句细粒度 kind（行内粗粒度 kind 仅 QUERY|COMMAND，见 OperationRow.Kind）。 */
  public enum Kind {
    QUERY,
    SET,
    RESET,
    SHOW,
    DESCRIBE,
    EXPLAIN,
    USE,
    DML,
    DDL
  }

  private static final Set<String> DESCRIBE_WORDS = Set.of("DESCRIBE", "DESC");
  private static final Set<String> DDL_WORDS = Set.of("CREATE", "DROP", "ALTER", "TRUNCATE");
  private static final Set<String> DML_WORDS = Set.of("INSERT", "UPDATE", "DELETE", "MERGE");

  private StatementClassifier() {}

  /** 分类：未知/空语句默认 QUERY。 */
  public static Kind classify(String sql) {
    String head = statementHead(sql);
    if (head.isEmpty()) {
      return Kind.QUERY;
    }
    String[] words = head.split("\\s+", 3);
    String first = words[0];
    String second = words.length > 1 ? words[1] : "";

    // 二词前缀：INSERT OVERWRITE（DML）/ CREATE OR REPLACE（DDL）
    if ("INSERT".equals(first) && "OVERWRITE".equals(second)) {
      return Kind.DML;
    }
    if ("CREATE".equals(first) && "OR".equals(second)) {
      return Kind.DDL;
    }

    if ("SET".equals(first)) {
      return Kind.SET;
    }
    if ("RESET".equals(first)) {
      return Kind.RESET;
    }
    if ("SHOW".equals(first)) {
      return Kind.SHOW;
    }
    if (DESCRIBE_WORDS.contains(first)) {
      return Kind.DESCRIBE;
    }
    if ("EXPLAIN".equals(first)) {
      return Kind.EXPLAIN;
    }
    if ("USE".equals(first)) {
      return Kind.USE;
    }
    if (DML_WORDS.contains(first)) {
      return Kind.DML;
    }
    if (DDL_WORDS.contains(first)) {
      return Kind.DDL;
    }
    return Kind.QUERY; // WITH…SELECT / SELECT / 未知 → QUERY
  }

  /** AnalyzePlan 可分析的 kind（可 inline 取真实 result schema）。 */
  public static boolean analyzable(Kind kind) {
    return kind == Kind.QUERY || kind == Kind.SHOW || kind == Kind.DESCRIBE || kind == Kind.EXPLAIN;
  }

  /**
   * 取语句头部词序列（大写、注释已剥、前导分号/空白已剥）。逐字符扫描：单引号/反引号/
   * 双引号内不剥注释（字符串里的 "--" 不是注释）；块注释 {@code /* *\/} 与行注释
   * {@code --} 剥到行尾。
   */
  private static String statementHead(String sql) {
    if (sql == null) {
      return "";
    }
    StringBuilder out = new StringBuilder();
    char quote = 0;
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (quote != 0) {
        if (c == quote) {
          quote = 0;
        }
        continue; // 引号内内容不进 head（首关键字不可能在字符串里）
      }
      if (c == '\'' || c == '"' || c == '`') {
        quote = c;
        continue;
      }
      if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
        while (i < sql.length() && sql.charAt(i) != '\n') {
          i++;
        }
        continue;
      }
      if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
        i += 2;
        while (i + 1 < sql.length() && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) {
          i++;
        }
        i++; // 越过 '/'
        continue;
      }
      if (c == ';') {
        // 首语句结束：分类只看首语句（多语句契约限制）
        if (out.toString().isBlank()) {
          continue; // 前导分号
        }
        break;
      }
      out.append(c);
    }
    return out.toString().strip().toUpperCase(Locale.ROOT);
  }
}
