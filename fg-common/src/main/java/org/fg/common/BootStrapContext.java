/*
 * Slimmed-down port of Dremio OSS BootStrapContext (sabot/kernel), Apache License 2.0:
 * config + root allocator + general context-migrating executor only.
 */
package org.fg.common;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.memory.BufferAllocator;
import org.fg.common.config.GatewayConfig;
import org.fg.common.concurrent.CloseableThreadPool;
import org.fg.common.concurrent.ContextMigratingExecutorService;
import org.fg.common.memory.RootAllocatorFactory;

/** Bootstrap context: config, root allocator and the general executor. */
public final class BootStrapContext implements AutoCloseable {

  private final GatewayConfig config;
  private final BufferAllocator allocator;
  private final ContextMigratingExecutorService.ContextMigratingCloseableExecutorService<
          CloseableThreadPool>
      executor;
  private final MeterRegistry meterRegistry;

  public BootStrapContext(GatewayConfig config) {
    this.config = config;
    this.allocator = RootAllocatorFactory.newRoot(config.getBytes(GatewayConfig.MEMORY_MAX));
    this.executor =
        new ContextMigratingExecutorService.ContextMigratingCloseableExecutorService<>(
            CloseableThreadPool.newCachedThreadPool("fg-general-"));
    this.meterRegistry = new SimpleMeterRegistry();
  }

  public BootStrapContext(
      GatewayConfig config, BufferAllocator allocator, MeterRegistry meterRegistry) {
    this.config = config;
    this.allocator = allocator;
    this.executor =
        new ContextMigratingExecutorService.ContextMigratingCloseableExecutorService<>(
            CloseableThreadPool.newCachedThreadPool("fg-general-"));
    this.meterRegistry = meterRegistry;
  }

  public GatewayConfig getConfig() {
    return config;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public ContextMigratingExecutorService getExecutor() {
    return executor;
  }

  public MeterRegistry getMeterRegistry() {
    return meterRegistry;
  }

  @Override
  public void close() throws Exception {
    executor.close();
    allocator.close();
    meterRegistry.close();
  }
}
