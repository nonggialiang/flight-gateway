package org.fg.result.ticket;

/**
 * HMAC 签名的自描述 ticket 信封（design H5/D13）：manifest 路径可直推（bucket +
 * resultKeyPrefix + "manifest.json"）。防伪造 + user 校验 + TTL 对齐。
 * partIndex 为 null 表示 STREAM（查询级）票；PART（分片级）票必填。
 */
public record RelayTicket(
    TicketKind kind,
    String bucket,
    String resultKeyPrefix,
    String queryId,
    String user,
    long issuedAtEpochSec,
    Integer partIndex) {

  public RelayTicket {
    if (kind == TicketKind.PART && (partIndex == null || partIndex < 0)) {
      throw new IllegalArgumentException("PART ticket requires non-negative partIndex");
    }
    if (kind == TicketKind.STREAM && partIndex != null) {
      throw new IllegalArgumentException("STREAM ticket must not carry partIndex");
    }
  }

  public String manifestObjectKey() {
    return resultKeyPrefix + "/manifest.json";
  }
}
