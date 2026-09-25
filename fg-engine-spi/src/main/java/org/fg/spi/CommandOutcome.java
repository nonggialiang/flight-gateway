package org.fg.spi;

/**
 * 命令（非 SELECT：SET/SHOW/DESCRIBE/EXPLAIN/USE/DDL/DML）执行结果（design D17/D18）。
 *
 * <p>命令结果不物化到对象存储——网关侧编排把 {@code resultIpcBytes}（完整 Arrow IPC
 * stream bytes：schema message + batch messages）内联落 fg_operation.command_result。
 * {@code schemaBytes} 为 schema message 序列化（与网关 SchemaSerde 字节一致，由 Kit 用
 * {@code MessageSerializer.serialize} 产出），终态 CAS 覆盖静态宣告 schema。网关持流至
 * 终态：流终即权威事实，无 UNKNOWN 态（区别于 attach 的 ExecutionOutcome——那边以
 * manifest 对账兜底）。
 */
public record CommandOutcome(OutcomeStatus status, String error, byte[] schemaBytes,
                             byte[] resultIpcBytes) {

  public enum OutcomeStatus {
    COMPLETED,
    FAILED,
    CANCELLED
  }

  public static CommandOutcome completed(byte[] schemaBytes, byte[] resultIpcBytes) {
    return new CommandOutcome(OutcomeStatus.COMPLETED, null, schemaBytes, resultIpcBytes);
  }

  public static CommandOutcome failed(String error) {
    return new CommandOutcome(OutcomeStatus.FAILED, error, null, null);
  }

  public static CommandOutcome cancelled() {
    return new CommandOutcome(OutcomeStatus.CANCELLED, null, null, null);
  }
}
