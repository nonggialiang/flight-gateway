package org.fg.spark.sink;

/**
 * sink 包内共享常量。{@link #BATCH_INDEX_SUFFIX}（D27）：per-part batch 索引边车后缀——
 * task 写完 part 后写 {@code {part}.bidx}（紧凑 JSON {@code {"batches":[[offset,length,rows],...]}}，
 * encapsulated message 自包含可独立 Range GET）；abort/purge 清理连带删除。网关侧同款
 * 推导（part uri + 后缀），缺席回落整 part 顺序读（优化非正确性依赖）。
 */
public final class FgResultSinks {

  /** batch 索引边车后缀。 */
  public static final String BATCH_INDEX_SUFFIX = ".bidx";

  private FgResultSinks() {}
}
