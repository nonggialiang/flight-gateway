package org.fg.frontend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.fg.common.config.GatewayConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** TLS 材料装配（D25）：配置校验快败 + KeyStore(PKCS12)→PEM 重编码通路。 */
class ServerTlsMaterialTest {

  private static final String[] KEYS = {
      "fg.flight.tls.cert-chain", "fg.flight.tls.private-key", "fg.flight.tls.key-store",
      "fg.flight.tls.key-store-password", "fg.flight.tls.key-store-alias",
      "fg.flight.tls.key-password", "fg.flight.tls.client-ca-cert",
  };

  @TempDir
  Path tmp;

  @BeforeEach
  @AfterEach
  void resetProps() {
    for (String k : KEYS) {
      System.clearProperty(k);
    }
  }

  private GatewayConfig config() {
    return GatewayConfig.create();
  }

  @Test
  void enabledWithoutMaterialFailsFast() {
    assertThatThrownBy(() -> ServerTlsMaterial.load(config()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("no material")
        .hasMessageContaining("cert-chain")
        .hasMessageContaining("key-store");
  }

  @Test
  void pemHalfConfiguredFailsFast() throws Exception {
    Path cert = tmp.resolve("server.crt");
    Files.writeString(cert, "-----BEGIN CERTIFICATE-----\n");
    System.setProperty("fg.flight.tls.cert-chain", cert.toString());
    assertThatThrownBy(() -> ServerTlsMaterial.load(config()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("both");
  }

  @Test
  void pemAndKeyStoreMutuallyExclusive() throws Exception {
    Path cert = tmp.resolve("server.crt");
    Files.writeString(cert, "x");
    Path store = tmp.resolve("server.p12");
    Files.write(store, new byte[1]);
    System.setProperty("fg.flight.tls.cert-chain", cert.toString());
    System.setProperty("fg.flight.tls.private-key", cert.toString());
    System.setProperty("fg.flight.tls.key-store", store.toString());
    assertThatThrownBy(() -> ServerTlsMaterial.load(config()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("mutually exclusive");
  }

  @Test
  void keyStoreMissingFileFailsFast() {
    System.setProperty("fg.flight.tls.key-store", tmp.resolve("nope.p12").toString());
    assertThatThrownBy(() -> ServerTlsMaterial.load(config()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not a file");
  }

  @Test
  void pkcs12KeyStoreIsReencodedAsPem() throws Exception {
    Path store = tmp.resolve("server.p12");
    keytool(store);
    System.setProperty("fg.flight.tls.key-store", store.toString());
    System.setProperty("fg.flight.tls.key-store-password", "fgpass");

    ServerTlsMaterial material = ServerTlsMaterial.load(config());
    String chain = readAll(material.certChain());
    String key = readAll(material.key());
    assertThat(chain).contains("-----BEGIN CERTIFICATE-----").contains("-----END CERTIFICATE-----");
    assertThat(key).contains("-----BEGIN PRIVATE KEY-----").contains("-----END PRIVATE KEY-----");
    assertThat(material.clientCaCert()).isEmpty();
  }

  @Test
  void wrongAliasFailsFast() throws Exception {
    Path store = tmp.resolve("server.p12");
    keytool(store);
    System.setProperty("fg.flight.tls.key-store", store.toString());
    System.setProperty("fg.flight.tls.key-store-password", "fgpass");
    System.setProperty("fg.flight.tls.key-store-alias", "no-such-alias");
    assertThatThrownBy(() -> ServerTlsMaterial.load(config()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("PrivateKey entry");
  }

  /** keytool 生成自签 RSA + SAN（测试期材料；生产证书走 PEM/受管 KeyStore）。 */
  private void keytool(Path store) throws Exception {
    String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
    List<String> cmd = new ArrayList<>(List.of(
        keytool, "-genkeypair", "-alias", "fg", "-keyalg", "RSA", "-keysize", "2048",
        "-validity", "30", "-dname", "CN=localhost",
        "-ext", "SAN=DNS:localhost,IP:127.0.0.1",
        "-keystore", store.toString(), "-storetype", "PKCS12",
        "-storepass", "fgpass", "-keypass", "fgpass", "-noprompt"));
    Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
    String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(p.waitFor()).as("keytool output: %s", out).isZero();
  }

  private static String readAll(InputStream in) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    in.transferTo(out);
    in.close();
    return out.toString(StandardCharsets.UTF_8);
  }
}
