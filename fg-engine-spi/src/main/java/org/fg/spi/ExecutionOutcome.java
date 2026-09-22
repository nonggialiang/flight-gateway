package org.fg.spi;

/** attach 流终止时的快速结果（权威事实以 manifest 为准，design D7）。 */
public record ExecutionOutcome(OutcomeStatus status, String error) {

  public enum OutcomeStatus {
    COMPLETED,
    FAILED,
    CANCELLED,
    /** 引擎不可达/流断且无法判定（等待对账兜底） */
    UNKNOWN
  }

  public static ExecutionOutcome completed() {
    return new ExecutionOutcome(OutcomeStatus.COMPLETED, null);
  }

  public static ExecutionOutcome failed(String error) {
    return new ExecutionOutcome(OutcomeStatus.FAILED, error);
  }

  public static ExecutionOutcome cancelled() {
    return new ExecutionOutcome(OutcomeStatus.CANCELLED, null);
  }

  public static ExecutionOutcome unknown(String error) {
    return new ExecutionOutcome(OutcomeStatus.UNKNOWN, error);
  }
}
