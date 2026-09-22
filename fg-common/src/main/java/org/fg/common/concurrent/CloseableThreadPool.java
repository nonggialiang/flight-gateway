/*
 * Ported from Dremio OSS (common/legacy com.dremio.common.concurrent.CloseableThreadPool),
 * Apache License 2.0.
 */
package org.fg.common.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Closeable thread pool: a ThreadPoolExecutor using a NamedThreadFactory and calling
 * awaitTermination on close.
 */
public class CloseableThreadPool extends ThreadPoolExecutor implements CloseableExecutorService {
  private static final Logger logger = LoggerFactory.getLogger(CloseableThreadPool.class);

  public static CloseableThreadPool newSingleThreadExecutor(String name) {
    return newFixedThreadPool(name, 1);
  }

  public static CloseableThreadPool newFixedThreadPool(String name, int fixedPoolSize) {
    return new CloseableThreadPool(
        name, fixedPoolSize, fixedPoolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
  }

  public static CloseableThreadPool newCachedThreadPool(String name) {
    // same params as java.util.concurrent.Executors.newCachedThreadPool
    return new CloseableThreadPool(
        name, 0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());
  }

  public CloseableThreadPool(
      String name,
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, new NamedThreadFactory(name));
  }

  @Override
  protected void afterExecute(final Runnable r, final Throwable t) {
    if (t != null) {
      logger.error("{}.run() leaked an exception.", r.getClass().getName(), t);
    }
    super.afterExecute(r, t);
  }

  @Override
  public void close() {
    CloseableSchedulerThreadPool.close(this, logger);
  }
}
