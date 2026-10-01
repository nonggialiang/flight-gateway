package org.fg.e2e;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * statementDefaultScroll 验证探针（fork 方案 A v2）： ① 默认连接（无参数）—— 无参
 * createStatement 为 FORWARD_ONLY、行为零变化； ② statementDefaultScroll=true —— 无参
 * createStatement/prepareStatement 自动 SCROLL_INSENSITIVE（服务端分页，翻页走短 DoGet），
 * 逐页遍历 500 行逐值；带参重载显式 FORWARD_ONLY 同样被升级（v2 连接级策略：DBeaver
 * 实证其执行语句经带参重载显式 FORWARD_ONLY，v1 无参覆写盖不住；逃生舱=不开 flag 的连接）。
 */
public final class StatementDefaultScrollProbe {
  public static void main(String[] args) throws Exception {
    String base = "jdbc:arrow-flight://localhost:32010?useEncryption=false";

    // ① 默认连接：无参 statement 仍 FORWARD_ONLY
    try (Connection conn = DriverManager.getConnection(base, props())) {
      try (Statement st = conn.createStatement()) {
        checkType(st, ResultSet.TYPE_FORWARD_ONLY, "默认连接无参 createStatement");
      }
      System.out.println("[sds] ① 默认连接 FORWARD_ONLY 不变 OK");
    }

    // ② statementDefaultScroll=true：无参升级 scroll + 翻页逐值（服务端分页）
    String scrollUrl = base + "&statementDefaultScroll=true";
    try (Connection conn = DriverManager.getConnection(scrollUrl, props())) {
      try (Statement st = conn.createStatement()) {
        checkType(st, ResultSet.TYPE_SCROLL_INSENSITIVE, "开参连接无参 createStatement");
        st.setFetchSize(50); // 页大小 50 → 500 行 = 10 页
        try (ResultSet rs = st.executeQuery("SELECT id, id * 2 AS dbl FROM range(500) ORDER BY id")) {
          long expect = 0;
          while (rs.next()) {
            if (rs.getLong(1) != expect || rs.getLong(2) != expect * 2) {
              throw new AssertionError("row " + rs.getRow() + ": " + rs.getLong(1));
            }
            expect++;
          }
          if (expect != 500) {
            throw new AssertionError("rows=" + expect);
          }
          // 跳页：absolute 回第 3 页
          if (!rs.absolute(201)) {
            throw new AssertionError("absolute(201)");
          }
          if (rs.getLong(1) != 200) {
            throw new AssertionError("absolute 后 id=" + rs.getLong(1));
          }
        }
      }
      try (java.sql.PreparedStatement ps = conn.prepareStatement("SELECT id FROM range(10)")) {
        // prepareStatement 无参重载同样升级：scroll 支持反向
        try (ResultSet rs = ps.executeQuery()) {
          rs.afterLast();
          int n = 0;
          while (rs.previous()) {
            n++;
          }
          if (n != 10) {
            throw new AssertionError("prepare scroll reverse n=" + n);
          }
        }
      }
      // v2：带参重载显式 FORWARD_ONLY 同样升级（连接级策略，覆盖 DBeaver 的建语句路径）
      try (Statement explicit =
          conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
        checkType(explicit, ResultSet.TYPE_SCROLL_INSENSITIVE, "开参连接显式 FORWARD_ONLY（v2 升级）");
      }
      System.out.println("[sds] ② 开参连接 scroll 升级 + 翻页/反向/带参 FORWARD_ONLY 升级 OK");
    }
    System.out.println("[sds] PASS");
  }

  private static Properties props() {
    Properties p = new Properties();
    p.setProperty("user", "fg");
    p.setProperty("password", "fg");
    return p;
  }

  private static void checkType(Statement st, int expected, String what) throws Exception {
    if (st.getResultSetType() != expected) {
      throw new AssertionError(
          what + ": type=" + st.getResultSetType() + " expected=" + expected);
    }
  }
}
