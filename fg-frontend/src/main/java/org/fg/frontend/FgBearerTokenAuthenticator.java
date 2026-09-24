package org.fg.frontend;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.fg.common.config.GatewayConfig;

/**
 * Bearer 优先 → Basic 回退 → 签发 token（design F1，照 Dremio DremioBearerTokenAuthenticator
 * 范式）：客户端 Basic 认证成功后签发随机 Bearer token（经 handshake/响应头回传，
 * AuthResult#appendToOutgoingHeaders），后续调用以 Bearer 携带；两种头都校验。
 *
 * <p>与 Dremio 差异：peerIdentity = username（FG 的票/行以 user 为语义，非 token）；
 * token→user 映射存进程内 Map（M1 单实例；M3 归 TokenManager + 轮换）。
 */
final class FgBearerTokenAuthenticator implements CallHeaderAuthenticator {

  private final CallHeaderAuthenticator initial;
  private final ConcurrentHashMap<String, String> tokenToUser = new ConcurrentHashMap<>();

  FgBearerTokenAuthenticator(GatewayConfig config) {
    this.initial = new BasicCallHeaderAuthenticator(new FgCredentialValidator(config));
  }

  @Override
  public AuthResult authenticate(CallHeaders incomingHeaders) {
    String bearerToken =
        AuthUtilities.getValueFromAuthHeader(incomingHeaders, Auth2Constants.BEARER_PREFIX);
    if (bearerToken != null) {
      String user = tokenToUser.get(bearerToken);
      if (user == null) {
        throw CallStatus.UNAUTHENTICATED.withDescription("Unknown bearer token").toRuntimeException();
      }
      return withBearer(bearerToken, user);
    }
    AuthResult basic = initial.authenticate(incomingHeaders); // Basic 校验，失败抛 UNAUTHENTICATED
    String user = basic.getPeerIdentity();
    String token = UUID.randomUUID().toString();
    tokenToUser.put(token, user);
    return withBearer(token, user);
  }

  private AuthResult withBearer(String token, String user) {
    return new AuthResult() {
      @Override
      public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
        outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER,
            Auth2Constants.BEARER_PREFIX + token);
      }

      @Override
      public String getPeerIdentity() {
        return user;
      }
    };
  }
}
