package org.fg.ha;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * 引擎注册 znode 的编解码（D22，Kyuubi znode 格式扩展 adminPort 属性）。
 *
 * <p>znode 名（EPHEMERAL_SEQUENTIAL 的 prefix，sequence 由 ZK 追加）：
 * <pre>
 * serverUri=host:connectPort;adminPort=N;version=V;refId=R;sequence=
 * </pre>
 * 内容（data）= {@code host:connectPort}（与 Kyuubi instance 同构）。属性解析容错
 * （{@code ;}/{@code =} 切分，未知属性忽略），serverUri 缺席时回落 data 内容。
 *
 * @param host        引擎可达地址（非回环）
 * @param connectPort Spark Connect gRPC 端口
 * @param adminPort   FgSessionAdmin HTTP 端口
 * @param version     注册版本（信息性）
 * @param refId       拉起请求 id（ensureEngine 轮询定位用）
 * @param sequence    ZK 追加序号（-1 = 未注册形态）
 * @param znodeName   原始 znode 名（删除守卫用，重建前为 null）
 */
public record EngineNode(
    String host, int connectPort, int adminPort, String version, String refId, long sequence, String znodeName) {

  /** 当前注册版本属性值。 */
  public static final String CURRENT_VERSION = "fg-0.1";

  /** 未注册形态（host/admin 端口已知，尚无 znode）。 */
  public static EngineNode of(String host, int connectPort, int adminPort, String version, String refId) {
    return new EngineNode(host, connectPort, adminPort, version, refId, -1L, null);
  }

  /** znode 名 prefix（EPHEMERAL_SEQUENTIAL 创建时用，sequence 段由 ZK 填充）。 */
  public static String znodePrefix(String host, int connectPort, int adminPort, String version, String refId) {
    return "serverUri=" + host + ":" + connectPort
        + ";adminPort=" + adminPort
        + ";version=" + (version == null || version.isBlank() ? CURRENT_VERSION : version)
        + ";refId=" + (refId == null ? "" : refId)
        + ";sequence=";
  }

  /** znode data 内容：{@code host:connectPort}。 */
  public static String instance(String host, int connectPort) {
    return host + ":" + connectPort;
  }

  /** 容错解析完整 znode 名（含 ZK 追加的 sequence）。 */
  public static EngineNode parse(String znodeName, byte[] data) {
    if (znodeName == null || znodeName.isBlank()) {
      throw new IllegalArgumentException("znodeName required");
    }
    String serverUri = null;
    String version = null;
    String refId = null;
    long sequence = -1L;
    int adminPort = -1;
    for (String part : znodeName.split(";")) {
      int i = part.indexOf('=');
      if (i <= 0) {
        continue;
      }
      String key = part.substring(0, i);
      String value = part.substring(i + 1);
      switch (key) {
        case "serverUri" -> serverUri = value;
        case "adminPort" -> adminPort = parseIntOr(value, -1);
        case "version" -> version = value;
        case "refId" -> refId = value;
        case "sequence" -> sequence = parseLongOr(value, -1L);
        default -> { /* 未知属性忽略（前向兼容） */ }
      }
    }
    if ((serverUri == null || serverUri.isBlank()) && data != null && data.length > 0) {
      serverUri = new String(data, StandardCharsets.UTF_8).trim();
    }
    if (serverUri == null || !serverUri.contains(":")) {
      throw new IllegalArgumentException("unparseable engine znode: " + znodeName);
    }
    int colon = serverUri.lastIndexOf(':');
    String host = serverUri.substring(0, colon);
    int connectPort = parseIntOr(serverUri.substring(colon + 1), -1);
    return new EngineNode(host, connectPort, adminPort,
        version == null || version.isBlank() ? CURRENT_VERSION : version,
        refId == null ? "" : refId, sequence, znodeName);
  }

  private static int parseIntOr(String v, int fallback) {
    try {
      return Integer.parseInt(v.trim());
    } catch (NumberFormatException e) {
      return fallback;
    }
  }

  private static long parseLongOr(String v, long fallback) {
    try {
      return Long.parseLong(v.trim());
    } catch (NumberFormatException e) {
      return fallback;
    }
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "%s:%d(admin=%d,refId=%s,seq=%d)",
        host, connectPort, adminPort, refId, sequence);
  }
}
