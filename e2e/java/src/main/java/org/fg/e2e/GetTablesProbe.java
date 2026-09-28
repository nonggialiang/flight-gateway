package org.fg.e2e;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

/** 最小探针：无 URL 参数（依赖 fork 驱动自动铸造 x-fg-session-id），只调 getTables。 */
public final class GetTablesProbe {
  public static void main(String[] args) throws Exception {
    String url = "jdbc:arrow-flight://localhost:32010?useEncryption=false";
    Properties props = new Properties();
    props.setProperty("user", "fg");
    props.setProperty("password", "fg");
    try (Connection conn = DriverManager.getConnection(url, props)) {
      System.out.println("[probe] connected");
      // 对照 ①：statement 路径（executeQuery）
      try (java.sql.Statement st = conn.createStatement();
          ResultSet r = st.executeQuery("SELECT 1")) {
        r.next();
        System.out.println("[probe] executeQuery OK");
      } catch (Exception e) {
        System.out.println("[probe] executeQuery FAILED: " + e.getMessage());
      }
      // 对照 ②：metadata 路径（getTables）
      try (ResultSet rs = conn.getMetaData().getTables(null, null, "%", null)) {
        int n = 0;
        while (rs.next()) {
          n++;
          if (n <= 3) {
            System.out.printf("[probe] table=%s type=%s%n", rs.getString("TABLE_NAME"),
                rs.getString("TABLE_TYPE"));
          }
        }
        System.out.println("[probe] getTables rows=" + n + " OK（无 URL 参数，自动铸造在位）");
      }
    }
  }
}
