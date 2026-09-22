package org.fg.spi;

/**
 * 物化指令：gateway 只描述"写到哪、什么格式"，不知道怎么写（design §4.3）。
 *
 * <p>partitionsHint 为写端分区控制（fg.result.partitions 的行级裁决结果，H4）：null=自然分区；
 * 1=coalesce(1) 单对象；K=repartition(K)。顺序敏感查询经 orchestrator 收敛（HTTPS 强制 1、
 * relay 禁 repartition）。
 *
 * <p>凭证 v1 不在 spec 内：引擎使用启动时注入的静态 scoped key（D5）。扩展预留：未来增加
 * credential 字段（每查询 presigned-PUT/STS）时只加字段，不破坏契约。
 */
public record MaterializationSpec(
    String objectUri,
    boolean zstdCompression,
    int maxRecordsPerBatchHint,
    Integer partitionsHint) {

  /** e.g. s3://bucket/prefix/{user}/{queryId}/ (always ends with '/') */
  public MaterializationSpec {
    if (objectUri == null || !objectUri.endsWith("/")) {
      throw new IllegalArgumentException("objectUri must end with '/': " + objectUri);
    }
  }

  public static MaterializationSpec of(
      String objectUri, boolean zstdCompression, int maxRecordsPerBatchHint) {
    return new MaterializationSpec(objectUri, zstdCompression, maxRecordsPerBatchHint, null);
  }
}
