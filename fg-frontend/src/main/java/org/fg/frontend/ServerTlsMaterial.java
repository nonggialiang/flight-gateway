package org.fg.frontend;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.Collections;
import java.util.Locale;
import java.util.Optional;
import org.fg.common.config.GatewayConfig;

/**
 * 服务端 TLS 材料装配（D25，Dremio SSLConfigurator 范式）：<b>PEM 双文件</b>或
 * <b>Java KeyStore</b>（PKCS12/JKS）二选一，可选 mTLS 客户端验证。
 *
 * <p>arrow FlightServer 的 gRPC 绑定只吃 PEM 流（{@code GrpcSslContexts.forServer(
 * InputStream, InputStream)}，FlightServer.Builder#build 内部转换）——KeyStore 材料
 * 在此重编码为 PEM 后交付（PKCS8 "PRIVATE KEY" + "CERTIFICATE" 链）。mTLS 经
 * {@code useMTlsClientVerification(CA 证书)}：ClientAuth.REQUIRE——客户端必须持该 CA
 * 签发的证书（Basic auth2 之外的设备级双栈，默认关闭）。
 *
 * <p>配置（fg.flight.tls.*）：
 * <pre>
 * enabled             开关（默认 false）
 * cert-chain          PEM 模式：证书链文件路径
 * private-key         PEM 模式：私钥文件路径（PKCS8/PKCS1 均可）
 * key-store           KeyStore 模式：PKCS12（.p12）或 JKS 文件路径
 * key-store-password  KeyStore 口令
 * key-store-alias     条目别名（空=取首个 PrivateKey 条目）
 * key-password        私钥口令（空=回落 key-store-password）
 * client-ca-cert      可选 mTLS：客户端证书 CA（PEM）
 * </pre>
 */
final class ServerTlsMaterial {

  private final InputStream certChain;
  private final InputStream key;
  private final File clientCaCert;

  private ServerTlsMaterial(InputStream certChain, InputStream key, File clientCaCert) {
    this.certChain = certChain;
    this.key = key;
    this.clientCaCert = clientCaCert;
  }

  /** PEM 证书链流（FlightServer build 时消费并关闭）。 */
  InputStream certChain() {
    return certChain;
  }

  /** PEM 私钥流（FlightServer build 时消费并关闭）。 */
  InputStream key() {
    return key;
  }

  /** mTLS CA 证书（缺席 = 不开客户端验证）。 */
  Optional<File> clientCaCert() {
    return Optional.ofNullable(clientCaCert);
  }

  /** 从配置装载（enabled=true 时调用；材料缺失/两模式混用即抛 IllegalStateException 快败）。 */
  static ServerTlsMaterial load(GatewayConfig config) {
    String certChainPath = path(config, "fg.flight.tls.cert-chain");
    String privateKeyPath = path(config, "fg.flight.tls.private-key");
    String keyStorePath = path(config, "fg.flight.tls.key-store");

    boolean pem = certChainPath != null || privateKeyPath != null;
    boolean store = keyStorePath != null;
    if (pem && store) {
      throw new IllegalStateException(
          "TLS misconfigured: fg.flight.tls.cert-chain/private-key (PEM) and fg.flight.tls.key-store"
              + " are mutually exclusive");
    }
    if (!pem && !store) {
      throw new IllegalStateException(
          "TLS enabled but no material: set fg.flight.tls.cert-chain + fg.flight.tls.private-key"
              + " (PEM) or fg.flight.tls.key-store (PKCS12/JKS)");
    }

    InputStream chain;
    InputStream keyStream;
    if (pem) {
      if (certChainPath == null || privateKeyPath == null) {
        throw new IllegalStateException(
            "TLS misconfigured: PEM mode requires both fg.flight.tls.cert-chain and"
                + " fg.flight.tls.private-key");
      }
      try {
        chain = new FileInputStream(certChainPath);
        keyStream = new FileInputStream(privateKeyPath);
      } catch (IOException e) {
        throw new IllegalStateException("TLS material unreadable: " + e.getMessage(), e);
      }
    } else {
      KeyStoreEntry entry = readKeyStore(config, keyStorePath);
      chain = new ByteArrayInputStream(entry.chainPem.getBytes(StandardCharsets.UTF_8));
      keyStream = new ByteArrayInputStream(
          toPem("PRIVATE KEY", entry.key.getEncoded()).getBytes(StandardCharsets.UTF_8));
    }

    File clientCa = path(config, "fg.flight.tls.client-ca-cert") == null
        ? null : new File(path(config, "fg.flight.tls.client-ca-cert"));
    if (clientCa != null && !clientCa.isFile()) {
      throw new IllegalStateException("fg.flight.tls.client-ca-cert not a file: " + clientCa);
    }
    return new ServerTlsMaterial(chain, keyStream, clientCa);
  }

  private static String path(GatewayConfig config, String key) {
    String v = config.hasPath(key) ? config.getString(key).trim() : "";
    return v.isEmpty() ? null : v;
  }

  private static KeyStoreEntry readKeyStore(GatewayConfig config, String keyStorePath) {
    File file = new File(keyStorePath);
    if (!file.isFile()) {
      throw new IllegalStateException("fg.flight.tls.key-store not a file: " + keyStorePath);
    }
    String storePassword = config.hasPath("fg.flight.tls.key-store-password")
        ? config.getString("fg.flight.tls.key-store-password") : "";
    String alias = path(config, "fg.flight.tls.key-store-alias");
    String keyPassword = config.hasPath("fg.flight.tls.key-password")
        && !config.getString("fg.flight.tls.key-password").isEmpty()
            ? config.getString("fg.flight.tls.key-password") : storePassword;
    String type = keyStorePath.toLowerCase(Locale.ROOT).endsWith(".jks") ? "JKS" : "PKCS12";
    try (InputStream in = new FileInputStream(file)) {
      KeyStore ks = KeyStore.getInstance(type);
      ks.load(in, storePassword.isEmpty() ? null : storePassword.toCharArray());
      String entryAlias = alias != null ? alias : firstKeyAlias(ks);
      if (entryAlias == null) {
        throw new IllegalStateException(
            "TLS key-store has no PrivateKey entry" + (alias != null ? " for alias '" + alias + "'" : ""));
      }
      KeyStore.ProtectionParameter protection =
          keyPassword.isEmpty() ? null : new KeyStore.PasswordProtection(keyPassword.toCharArray());
      KeyStore.Entry entry = ks.getEntry(entryAlias, protection);
      if (!(entry instanceof KeyStore.PrivateKeyEntry)) {
        throw new IllegalStateException(
            "TLS key-store alias '" + entryAlias + "' is not a PrivateKey entry");
      }
      KeyStore.PrivateKeyEntry pkEntry = (KeyStore.PrivateKeyEntry) entry;
      StringBuilder chainPem = new StringBuilder();
      for (Certificate cert : pkEntry.getCertificateChain()) {
        chainPem.append(toPem("CERTIFICATE", cert.getEncoded()));
      }
      return new KeyStoreEntry((PrivateKey) pkEntry.getPrivateKey(), chainPem.toString());
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalStateException(
          "TLS key-store unreadable (" + type + " " + keyStorePath + "): " + e.getMessage(), e);
    }
  }

  private static String firstKeyAlias(KeyStore ks) throws GeneralSecurityException {
    for (String a : Collections.list(ks.aliases())) {
      if (ks.isKeyEntry(a)) {
        return a;
      }
    }
    return null;
  }

  /** DER → PEM（64 列 base64，PKCS8 私钥/CERTIFICATE 证书通用）。 */
  private static String toPem(String type, byte[] der) {
    StringBuilder sb = new StringBuilder();
    sb.append("-----BEGIN ").append(type).append("-----\n");
    sb.append(Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der));
    sb.append("\n-----END ").append(type).append("-----\n");
    return sb.toString();
  }

  private static final class KeyStoreEntry {
    final PrivateKey key;
    /** 证书链 PEM（逐证书块拼接——GrpcSslContexts 按块解析链序）。 */
    final String chainPem;

    KeyStoreEntry(PrivateKey key, String chainPem) {
      this.key = key;
      this.chainPem = chainPem;
    }
  }
}
