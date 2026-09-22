package org.fg.spi;

/**
 * 物化指令：gateway 只描述"写到哪、什么格式"，不知道怎么写（design §4.3）。
 *
 * <p>凭证 v1 不在 spec 内：引擎使用启动时注入的静态 scoped key 访问对象存储（引擎级配置，见 D5）。
 * 扩展预留：未来增加 credential 字段（每查询 presigned-PUT/STS）时只加字段，不破坏契约。
 */
public record MaterializationSpec(
    String objectUri, boolean zstdCompression, int maxRecordsPerBatchHint) {

  /** e.g. s3://bucket/prefix/{user}/{queryId}/ (always ends with '/') */
  public MaterializationSpec {
    if (objectUri == null || !objectUri.endsWith("/")) {
      throw new IllegalArgumentException("objectUri must end with '/': " + objectUri);
    }
  }
}
