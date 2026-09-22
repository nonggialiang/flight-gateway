package org.fg.result.ticket;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * RelayTicket 的 HMAC-SHA256 编解码（design D13）。
 *
 * <p>负载紧凑编码：{@code kind|bucket|resultKeyPrefix|queryId|user|issuedAt|partIndex(- 表示无)}，
 * 签名 hex 追加。支持双密钥轮换窗口（新密钥签发、新/旧密钥皆可验证）。密钥轮换失效与 TTL 对齐。
 */
public final class RelayTicketCodec {

  private static final char SEP = '|';
  private static final String HMAC_ALGORITHM = "HmacSHA256";

  private final byte[] currentKey;
  private final byte[] previousKey; // nullable, for rotation window
  private final Duration ttl;

  public RelayTicketCodec(String currentKey, String previousKey, Duration ttl) {
    this.currentKey = keyBytes(currentKey);
    this.previousKey = previousKey == null || previousKey.isBlank() ? null : keyBytes(previousKey);
    this.ttl = ttl;
  }

  private static byte[] keyBytes(String key) {
    return key.getBytes(StandardCharsets.UTF_8);
  }

  public String encode(RelayTicket ticket) {
    String payload = payload(ticket);
    return payload + SEP + sign(payload, currentKey);
  }

  /**
   * 验签 + TTL + user 校验。
   *
   * @throws InvalidTicketException 验签失败/过期/user 不匹配/格式非法
   */
  public RelayTicket decode(String encoded, String expectedUser) {
    int lastSep = encoded.lastIndexOf(SEP);
    if (lastSep <= 0) {
      throw new InvalidTicketException("Malformed ticket");
    }
    String payload = encoded.substring(0, lastSep);
    String signature = encoded.substring(lastSep + 1);

    boolean valid = verify(payload, signature, currentKey)
        || (previousKey != null && verify(payload, signature, previousKey));
    if (!valid) {
      throw new InvalidTicketException("Bad signature");
    }

    String[] fields = payload.split("\\" + SEP, -1);
    if (fields.length != 7) {
      throw new InvalidTicketException("Malformed payload");
    }
    TicketKind kind;
    try {
      kind = TicketKind.valueOf(fields[0]);
    } catch (IllegalArgumentException e) {
      throw new InvalidTicketException("Unknown kind: " + fields[0]);
    }
    long issuedAt = parseLong(fields[5]);
    long now = System.currentTimeMillis() / 1000;
    if (now - issuedAt > ttl.toSeconds()) {
      throw new ExpiredTicketException("Ticket expired");
    }
    if (!fields[4].equals(expectedUser)) {
      throw new InvalidTicketException("Ticket user mismatch");
    }
    Integer partIndex = fields[6].equals("-") ? null : parseOptionalInt(fields[6]);
    return new RelayTicket(kind, fields[1], fields[2], fields[3], fields[4], issuedAt, partIndex);
  }

  private static long parseLong(String s) {
    try {
      return Long.parseLong(s);
    } catch (NumberFormatException e) {
      throw new InvalidTicketException("Malformed issuedAt: " + s);
    }
  }

  private static Integer parseOptionalInt(String s) {
    try {
      return Integer.parseInt(s);
    } catch (NumberFormatException e) {
      throw new InvalidTicketException("Malformed partIndex: " + s);
    }
  }

  private static String payload(RelayTicket t) {
    return t.kind().name()
        + SEP
        + t.bucket()
        + SEP
        + t.resultKeyPrefix()
        + SEP
        + t.queryId()
        + SEP
        + t.user()
        + SEP
        + t.issuedAtEpochSec()
        + SEP
        + (t.partIndex() == null ? "-" : t.partIndex());
  }

  private boolean verify(String payload, String signature, byte[] key) {
    String expected = sign(payload, key);
    return MessageDigest.isEqual(
        expected.getBytes(StandardCharsets.UTF_8), signature.getBytes(StandardCharsets.UTF_8));
  }

  private static String sign(String payload, byte[] key) {
    try {
      Mac mac = Mac.getInstance(HMAC_ALGORITHM);
      mac.init(new SecretKeySpec(key, HMAC_ALGORITHM));
      return HexFormat.of().formatHex(mac.doFinal(payload.getBytes(StandardCharsets.UTF_8)));
    } catch (InvalidKeyException | java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException("Unable to sign ticket", e);
    }
  }

  /** ticket 校验失败（签名/格式/user）。 */
  public static class InvalidTicketException extends RuntimeException {
    public InvalidTicketException(String message) {
      super(message);
    }
  }

  /** ticket 过期（client 应重新 poll 拿新 endpoints / RenewFlightEndpoint）。 */
  public static class ExpiredTicketException extends InvalidTicketException {
    public ExpiredTicketException(String message) {
      super(message);
    }
  }
}
