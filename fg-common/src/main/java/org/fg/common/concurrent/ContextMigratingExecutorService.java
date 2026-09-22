/*
 * Ported from Dremio OSS (common/legacy com.dremio.common.concurrent.ContextMigratingExecutorService),
 * Apache License 2.0. Trimmed: invoke* delegate without context decoration.
 */
package org.fg.common.concurrent;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.fg.common.context.RequestContext;

/**
 * ExecutorService that migrates the current {@link RequestContext} from the submitting thread to
 * the pool thread executing the task.
 */
public class ContextMigratingExecutorService implements ExecutorService {

  private final ExecutorService delegate;

  public ContextMigratingExecutorService(ExecutorService delegate) {
    this.delegate = delegate;
  }

  /** Wraps the runnable such that the submitting thread's context is active during execution. */
  public static Runnable makeContextMigratingTask(final Runnable inner, final String taskName) {
    final RequestContext context = RequestContext.current();
    return () -> context.run(inner);
  }

  public static <V> Callable<V> makeContextMigratingTask(final Callable<V> inner) {
    final RequestContext context = RequestContext.current();
    return () -> {
      final V[] result = (V[]) new Object[1];
      context.run(() -> {
        try {
          result[0] = inner.call();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      return result[0];
    };
  }

  @Override
  public void execute(Runnable command) {
    delegate.execute(makeContextMigratingTask(command, "execute"));
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return delegate.submit(makeContextMigratingTask(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return delegate.submit(makeContextMigratingTask(task, "submit"), result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return delegate.submit(makeContextMigratingTask(task, "submit"));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return delegate.invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    return delegate.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return delegate.invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return delegate.invokeAny(tasks, timeout, unit);
  }

  @Override
  public void shutdown() {
    delegate.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return delegate.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return delegate.awaitTermination(timeout, unit);
  }

  /** Adds close semantics on top of context migration. */
  public static class ContextMigratingCloseableExecutorService<
          C extends AutoCloseable & ExecutorService>
      extends ContextMigratingExecutorService implements CloseableExecutorService {

    private final C closeable;

    public ContextMigratingCloseableExecutorService(C closeable) {
      super(closeable);
      this.closeable = closeable;
    }

    @Override
    public void close() throws Exception {
      closeable.close();
    }
  }
}
