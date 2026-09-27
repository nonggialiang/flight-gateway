package org.fg.frontend;

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.fg.common.config.GatewayConfig;

/**
 * auth2（CallHeaderAuthenticator）凭据校验（design F1）：与 auth1（FgBasicAuthValidator）
 * 同源配置、同语义（peerIdentity = username），服务 JDBC 驱动（flight-sql-jdbc-driver 走
 * Authorization: Basic 头握手，不认 auth1 的 BasicAuth protobuf 载荷）。
 *
 * <p>1-stage 模式：不签发 Bearer token（appendToOutgoingHeaders 留默认），客户端每调用
 * 附带 Basic 凭据；token 签发/轮换归 M3。
 *
 * <p>多用户（D22 USER share e2e）：可选 {@code fg.auth.basic.users}（"user:pass" 列表或
 * 逗号串）；空则回落单键对 {@code fg.auth.basic.username/password}（M1 兼容）。
 */
final class FgCredentialValidator implements BasicCallHeaderAuthenticator.CredentialValidator {

  private final GatewayConfig config;
  private final Map<String, String> users;

  FgCredentialValidator(GatewayConfig config) {
    this.config = config;
    this.users = parseUsers(config);
  }

  private static Map<String, String> parseUsers(GatewayConfig config) {
    Map<String, String> out = new HashMap<>();
    if (config.hasPath("fg.auth.basic.users")) {
      for (String entry : config.getStringList("fg.auth.basic.users")) {
        String e = entry == null ? "" : entry.trim();
        if (e.isEmpty()) {
          continue;
        }
        int i = e.indexOf(':');
        if (i <= 0 || i == e.length() - 1) {
          continue; // 容错跳过畸形项（启动期配置面，无需 fast-fail）
        }
        out.put(e.substring(0, i), e.substring(i + 1));
      }
    }
    return out;
  }

  @Override
  public CallHeaderAuthenticator.AuthResult validate(String username, String password) {
    boolean ok;
    if (users.isEmpty()) {
      // 单键对模式（M1 默认）
      String expectedUser = config.getString("fg.auth.basic.username");
      String expectedPass = config.getString("fg.auth.basic.password");
      ok = expectedUser.equals(username) && expectedPass.equals(password);
    } else {
      ok = password != null && password.equals(users.get(username));
    }
    if (!ok) {
      throw new SecurityException("Invalid credentials");
    }
    return () -> username;
  }
}
