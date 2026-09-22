package org.fg.frontend;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.flight.auth.BasicServerAuthHandler.BasicAuthValidator;
import org.fg.common.config.GatewayConfig;

/**
 * Basic token 认证（design F1）：用户名/密码经配置校验，签发随机 token；peerIdentity=user。
 * bearer/cookie 双轨（Dremio BearerTokenAuthenticator 范式）留 M3 强化。
 */
final class FgBasicAuthValidator implements BasicAuthValidator {

  private final GatewayConfig config;
  private final ConcurrentHashMap<String, String> tokenToUser = new ConcurrentHashMap<>();

  FgBasicAuthValidator(GatewayConfig config) {
    this.config = config;
  }

  @Override
  public byte[] getToken(String username, String password) throws Exception {
    String expectedUser = config.getString("fg.auth.basic.username");
    String expectedPass = config.getString("fg.auth.basic.password");
    if (!expectedUser.equals(username) || !expectedPass.equals(password)) {
      throw new SecurityException("Invalid credentials");
    }
    String token = UUID.randomUUID().toString();
    tokenToUser.put(token, username);
    return token.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public Optional<String> isValid(byte[] token) {
    if (token == null) {
      return Optional.empty();
    }
    String user = tokenToUser.get(new String(token, StandardCharsets.UTF_8));
    if (user == null) {
      org.slf4j.LoggerFactory.getLogger(FgBasicAuthValidator.class)
          .warn("Unknown token presented (len={})", token.length);
    }
    return Optional.ofNullable(user);
  }
}
