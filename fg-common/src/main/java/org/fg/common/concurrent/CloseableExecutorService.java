/*
 * Ported from Dremio OSS (common/legacy com.dremio.common.concurrent.CloseableExecutorService),
 * Apache License 2.0.
 */
package org.fg.common.concurrent;

import java.util.concurrent.ExecutorService;

/** An ExecutorService that can be closed. */
public interface CloseableExecutorService extends ExecutorService, AutoCloseable {}
