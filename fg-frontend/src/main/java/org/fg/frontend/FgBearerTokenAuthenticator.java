package org.fg.frontend;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.AuthUtilities;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.fg.common.config.GatewayConfig;

/**
 * Bearer 优先 → Basic 回退 → 签发 token（design F1，照 Dremio DremioBearerTokenAuthenticator
 * 范式）：客户端 Basic 认证成功后签发 Bearer token（经 handshake/响应头回传，
 * AuthResult#appendToOutgoingHeaders），后续调用以 Bearer 携带；两种头都校验。
 *
 * <p><b>无状态签名 token（重启/多实例兼容）</b>：token 自带身份与 HMAC 签名
 * （`fg1.&lt;b64user&gt;.&lt;b64expiry&gt;.&lt;b64hmac&gt;`），验证只需共享密钥——不依赖进程内
 * Map。此前的 ConcurrentHashMap 实现有两个失效面：网关重启 → 全量已发 token 失效；
 * 多实例部署 → 连接迁到未签发该 token 的实例即 "Unknown bearer token"（客户端不会自动
 * 重新握手，连接直接死）。密钥 {@code fg.auth.token.secret}（空回落
 * {@code fg.result.ticket.secret}——网关级共享密钥，多实例须一致）；TTL
 * {@code fg.auth.token.ttl}（默认 0=不过期；注意客户端无 401 重试，启用 TTL 意味着
 * 到期连接需重连）。撤销/轮换归 M3 TokenManager。
 */
final class FgBearerTokenAuthenticator implements CallHeaderAuthenticator {

  private static final String TOKEN_PREFIX = "fg1.";
  private static final Base64.Encoder B64 = Base64.getUrlEncoder().withoutPadding();
  private static final Base64.Decoder B64D = Base64.getUrlDecoder();

  private final CallHeaderAuthenticator initial;
  private final byte[] secret;
  private final long ttlSeconds;

  FgBearerTokenAuthenticator(GatewayConfig config) {
    this.initial = new BasicCallHeaderAuthenticator(new FgCredentialValidator(config));
    String secret =
        config.hasPath("fg.auth.token.secret") && !config.getString("fg.auth.token.secret").trim().isEmpty()
            ? config.getString("fg.auth.token.secret").trim()
            : config.getString("fg.result.ticket.secret"); // 网关级共享密钥兜底
    this.secret = secret.getBytes(StandardCharsets.UTF_8);
    this.ttlSeconds =
        config.hasPath("fg.auth.token.ttl") ? config.getDurationMs("fg.auth.token.ttl") / 1000 : 0L;
  }

  @Override
  public AuthResult authenticate(CallHeaders incomingHeaders) {
    String bearerToken =
        AuthUtilities.getValueFromAuthHeader(incomingHeaders, Auth2Constants.BEARER_PREFIX);
    if (bearerToken != null) {
      String user = verify(bearerToken);
      if (user == null) {
        throw CallStatus.UNAUTHENTICATED
            .withDescription("Invalid or expired bearer token")
            .toRuntimeException();
      }
      return withBearer(bearerToken, user);
    }
    AuthResult basic = initial.authenticate(incomingHeaders); // Basic 校验，失败抛 UNAUTHENTICATED
    String user = basic.getPeerIdentity();
    return withBearer(mint(user), user);
  }

  /** 签发：fg1.<b64user>.<b64expiryEpochSec(0=不过期)>.<b64hmac(payload)> */
  private String mint(String user) {
    long expiry = ttlSeconds > 0 ? System.currentTimeMillis() / 1000 + ttlSeconds : 0L;
    String payload = B64.encodeToString(user.getBytes(StandardCharsets.UTF_8))
        + "." + B64.encodeToString(Long.toString(expiry).getBytes(StandardCharsets.UTF_8));
    return TOKEN_PREFIX + payload + "." + B64.encodeToString(hmac(payload));
  }

  /** 验证：签名恒时比较 + TTL；任何不合法返回 null（由调用方统一 UNAUTHENTICATED）。 */
  private String verify(String token) {
    if (token == null || !token.startsWith(TOKEN_PREFIX)) {
      return null;
    }
    String rest = token.substring(TOKEN_PREFIX.length());
    int i = rest.lastIndexOf('.');
    if (i <= 0) {
      return null;
    }
    String payload = rest.substring(0, i);
    byte[] sig;
    try {
      sig = B64D.decode(rest.substring(i + 1));
    } catch (IllegalArgumentException e) {
      return null;
    }
    if (!MessageDigest.isEqual(hmac(payload), sig)) {
      return null;
    }
    try {
      int j = payload.lastIndexOf('.');
      String userB64 = payload.substring(0, j);
      long expiry = Long.parseLong(new String(B64D.decode(payload.substring(j + 1)), StandardCharsets.UTF_8));
      if (expiry > 0 && System.currentTimeMillis() / 1000 >= expiry) {
        return null;
      }
      return new String(B64D.decode(userB64), StandardCharsets.UTF_8);
    } catch (Exception e) {
      return null;
    }
  }

  private byte[] hmac(String payload) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(secret, "HmacSHA256"));
      return mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new IllegalStateException("HMAC unavailable", e);
    }
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
