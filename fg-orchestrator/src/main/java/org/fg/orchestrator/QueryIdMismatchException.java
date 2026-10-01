package org.fg.orchestrator;

/**
 * D28 ①'：x-fg-query-id 命中的行与本次 SQL 指纹不符（旧票配新 SQL = fork 驱动 bug）。
 * 显式报错而非静默落穿——frontend 映射为 INVALID_ARGUMENT。
 */
public class QueryIdMismatchException extends IllegalStateException {
  public QueryIdMismatchException(String message) {
    super(message);
  }
}
