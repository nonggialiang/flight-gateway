/*
 * Slimmed-down port of Dremio OSS BootStrapContext (sabot/kernel), Apache License 2.0:
 * config + root allocator + general context-migrating executor + metrics registry.
 */
package org.fg.common;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.fg.common.config.GatewayConfig;
import org.fg.common.concurrent.CloseableThreadPool;
import org.fg.common.concurrent.ContextMigratingExecutorService;
import org.fg.common.memory.RootAllocatorFactory;

/** Bootstrap context: config, root allocator, the general executor and the metrics registry. */
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
    // D26：metrics.enabled（默认开）→ PrometheusMeterRegistry（/metrics 暴露由
    // MetricsHttpServer 服务承接，装配见 GatewayDaemonModule）；关闭 → Simple 兜底
    this.meterRegistry =
        config.getBoolean("fg.metrics.enabled")
            ? new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
            : new SimpleMeterRegistry();
    bindJvmMetrics(meterRegistry);
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

  /** JVM 基线指标（内存/GC/线程/处理器/fd/uptime——micrometer-core 自带 binder）。 */
  private static void bindJvmMetrics(MeterRegistry registry) {
    new JvmMemoryMetrics().bindTo(registry);
    new JvmGcMetrics().bindTo(registry);
    new JvmThreadMetrics().bindTo(registry);
    new ProcessorMetrics().bindTo(registry);
    new FileDescriptorMetrics().bindTo(registry);
    new UptimeMetrics().bindTo(registry);
  }
}
