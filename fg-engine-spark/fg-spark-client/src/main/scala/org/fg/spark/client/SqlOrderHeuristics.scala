package org.fg.spark.client

/** 顶层 ORDER BY 检测（H4）：忽略字符串字面量/注释/括号内子查询的启发式扫描。 */
object SqlOrderHeuristics {

  def isOrderSensitive(sql: String): Boolean = {
    val s = stripComments(sql)
    var depth = 0
    var i = 0
    var inSingle = false
    var inDouble = false
    val n = s.length
    while (i < n) {
      val c = s.charAt(i)
      if (inSingle) {
        if (c == '\'') inSingle = false
      } else if (inDouble) {
        if (c == '"') inDouble = false
      } else {
        c match {
          case '\'' => inSingle = true
          case '"' => inDouble = true
          case '(' => depth += 1
          case ')' => depth -= 1
          case _ =>
            if (depth == 0
                && (c == 'O' || c == 'o')
                && regionMatchesIgnoreCase(s, i, "ORDER", 0, 5)
                && isWordBoundary(s, i - 1)
                && isWordBoundary(s, i + 5)) {
              var after = i + 5
              while (after < n && s.charAt(after) == ' ') after += 1 // 跳过空格
              if (matchesAt(s, after, "BY") && isWordBoundary(s, after + 2)) {
                return true
              }
            }
        }
      }
      i += 1
    }
    false
  }

  private def stripComments(sql: String): String =
    sql.replaceAll("--[^\n]*", " ").replaceAll("/\\*.*?\\*/", " ")

  private def regionMatchesIgnoreCase(s: String, i: Int, kw: String, off: Int, len: Int): Boolean = {
    val end = i + len
    if (end > s.length) return false
    var j = i
    var k = off
    while (j < end) {
      if (Character.toUpperCase(s.charAt(j)) != kw.charAt(k)) return false
      j += 1
      k += 1
    }
    true
  }

  private def matchesAt(s: String, i: Int, kw: String): Boolean = {
    val end = i + kw.length
    if (end > s.length) return false
    var j = i
    while (j < end) {
      if (Character.toUpperCase(s.charAt(j)) != kw.charAt(j - i)) return false
      j += 1
    }
    true
  }

  private def isWordBoundary(s: String, idx: Int): Boolean = {
    if (idx < 0 || idx >= s.length) true
    else !Character.isLetterOrDigit(s.charAt(idx))
  }
}
