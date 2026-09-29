package org.fg.frontend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator.AuthResult;
import org.fg.common.config.GatewayConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 无状态 Bearer token（重启/多实例兼容）：签发/验证往返、跨"实例"验证（同密钥新
 * authenticator）、篡改/错密钥拒绝、Basic 回退与错误凭证、TTL。
 */
class FgBearerTokenAuthenticatorTest {

  private static final String[] KEYS = {"fg.auth.token.secret", "fg.auth.token.ttl"};

  @AfterEach
  void tearDown() {
    for (String k : KEYS) {
      System.clearProperty(k);
    }
  }

  private GatewayConfig config() {
    return GatewayConfig.create();
  }

  private static CallHeaders basic(String user, String pass) {
    FlightCallHeaders h = new FlightCallHeaders();
    h.insert(Auth2Constants.AUTHORIZATION_HEADER,
        Auth2Constants.BASIC_PREFIX
            + java.util.Base64.getEncoder()
                .encodeToString((user + ":" + pass).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    return h;
  }

  private static CallHeaders bearer(String token) {
    FlightCallHeaders h = new FlightCallHeaders();
    h.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + token);
    return h;
  }

  @Test
  void basicMintThenBearerRoundTrip() {
    FgBearerTokenAuthenticator auth = new FgBearerTokenAuthenticator(config());
    AuthResult minted = auth.authenticate(basic("fg", "fg"));
    assertThat(minted.getPeerIdentity()).isEqualTo("fg");

    // token 经 outgoing header 交予客户端；携 token 再来 → 同身份
    FlightCallHeaders outgoing = new FlightCallHeaders();
    minted.appendToOutgoingHeaders(outgoing);
    String token = outgoing.get(Auth2Constants.AUTHORIZATION_HEADER)
        .substring(Auth2Constants.BEARER_PREFIX.length());
    assertThat(auth.authenticate(bearer(token)).getPeerIdentity()).isEqualTo("fg");
  }

  @Test
  void tokenValidAcrossInstancesAndRestarts() {
    // "实例 A" 签发；"实例 B"（同密钥的新进程）验证 —— 多实例与重启后的共同抽象
    FgBearerTokenAuthenticator instanceA = new FgBearerTokenAuthenticator(config());
    FlightCallHeaders outgoing = new FlightCallHeaders();
    instanceA.authenticate(basic("fg", "fg")).appendToOutgoingHeaders(outgoing);
    String token = outgoing.get(Auth2Constants.AUTHORIZATION_HEADER)
        .substring(Auth2Constants.BEARER_PREFIX.length());

    FgBearerTokenAuthenticator instanceB = new FgBearerTokenAuthenticator(config());
    assertThat(instanceB.authenticate(bearer(token)).getPeerIdentity()).isEqualTo("fg");
  }

  @Test
  void tamperedTokenAndWrongSecretRejected() {
    FgBearerTokenAuthenticator auth = new FgBearerTokenAuthenticator(config());
    FlightCallHeaders outgoing = new FlightCallHeaders();
    auth.authenticate(basic("fg", "fg")).appendToOutgoingHeaders(outgoing);
    String token = outgoing.get(Auth2Constants.AUTHORIZATION_HEADER)
        .substring(Auth2Constants.BEARER_PREFIX.length());

    // 篡改 payload（改身份段）
    String tampered = token.substring(0, token.indexOf('.')) + ".YWxpY2U." + token.split("\\.")[3];
    assertThatThrownBy(() -> auth.authenticate(bearer(tampered)))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Invalid or expired bearer token");

    // 不同密钥的实例拒绝
    System.setProperty("fg.auth.token.secret", "another-secret");
    FgBearerTokenAuthenticator otherCluster = new FgBearerTokenAuthenticator(config());
    assertThatThrownBy(() -> otherCluster.authenticate(bearer(token)))
        .hasMessageContaining("Invalid or expired bearer token");

    // 非 token 格式（旧版随机 UUID）同样拒绝
    assertThatThrownBy(() -> auth.authenticate(bearer(java.util.UUID.randomUUID().toString())))
        .hasMessageContaining("Invalid or expired bearer token");
  }

  @Test
  void badBasicCredentialsRejected() {
    FgBearerTokenAuthenticator auth = new FgBearerTokenAuthenticator(config());
    // arrow BasicCallHeaderAuthenticator 将 SecurityException 包成 UNAUTHENTICATED
    // FlightRuntimeException（原始 message 丢弃），此处断言类型语义即可
    assertThatThrownBy(() -> auth.authenticate(basic("fg", "wrong")))
        .isInstanceOf(org.apache.arrow.flight.FlightRuntimeException.class);
  }

  @Test
  void ttlExpiryEnforced() throws Exception {
    System.setProperty("fg.auth.token.ttl", "1s");
    FgBearerTokenAuthenticator auth = new FgBearerTokenAuthenticator(config());
    FlightCallHeaders outgoing = new FlightCallHeaders();
    auth.authenticate(basic("fg", "fg")).appendToOutgoingHeaders(outgoing);
    String token = outgoing.get(Auth2Constants.AUTHORIZATION_HEADER)
        .substring(Auth2Constants.BEARER_PREFIX.length());
    assertThat(auth.authenticate(bearer(token)).getPeerIdentity()).isEqualTo("fg");
    Thread.sleep(1200);
    assertThatThrownBy(() -> auth.authenticate(bearer(token)))
        .hasMessageContaining("Invalid or expired bearer token");
  }
}
