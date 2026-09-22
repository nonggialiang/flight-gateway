/*
 * Ported from Dremio OSS (common/legacy com.dremio.common.concurrent.CloseableSchedulerThreadPool),
 * Apache License 2.0.
 */
package org.fg.common.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

/** Utility for closing executor services gracefully. */
public final class CloseableSchedulerThreadPool {
  private static final long MAX_TIME_TO_WAIT_IN_MS = 30_000;

  private CloseableSchedulerThreadPool() {}

  /**
   * Closes given {@link ExecutorService} by first calling shutdown() and then, if needed,
   * shutdownNow(). Waits up to {@value #MAX_TIME_TO_WAIT_IN_MS}ms for termination.
   */
  public static void close(ExecutorService executor, Logger logger) {
    if (executor == null) {
      return;
    }
    executor.shutdown();
    boolean terminated = false;
    try {
      terminated = executor.awaitTermination(MAX_TIME_TO_WAIT_IN_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      if (!terminated) {
        logger.warn("Forcing shutdown of {}", executor);
        executor.shutdownNow();
      }
    }
  }
}
