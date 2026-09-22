/*
 * Ported from Dremio OSS (common/legacy com.dremio.common.concurrent.NamedThreadFactory),
 * Apache License 2.0.
 */
package org.fg.common.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ThreadFactory} that names threads sequentially with the given prefix. Created threads are
 * daemon threads.
 */
public class NamedThreadFactory implements ThreadFactory {
  private static final Logger logger = LoggerFactory.getLogger(NamedThreadFactory.class);
  private final AtomicInteger nextId = new AtomicInteger();
  private final String prefix;

  public NamedThreadFactory(final String prefix) {
    this.prefix = prefix;
  }

  @Override
  public Thread newThread(final Runnable runnable) {
    final Thread thread = new Thread(runnable, prefix + nextId.incrementAndGet());
    thread.setDaemon(true);
    return thread;
  }
}
