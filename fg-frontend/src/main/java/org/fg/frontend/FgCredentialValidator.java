package org.fg.frontend;

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
 */
final class FgCredentialValidator implements BasicCallHeaderAuthenticator.CredentialValidator {

  private final GatewayConfig config;

  FgCredentialValidator(GatewayConfig config) {
    this.config = config;
  }

  @Override
  public CallHeaderAuthenticator.AuthResult validate(String username, String password) {
    String expectedUser = config.getString("fg.auth.basic.username");
    String expectedPass = config.getString("fg.auth.basic.password");
    if (!expectedUser.equals(username) || !expectedPass.equals(password)) {
      throw new SecurityException("Invalid credentials");
    }
    return () -> username;
  }
}
