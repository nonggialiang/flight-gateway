/*
 * Ported from Dremio OSS (common/legacy org.apache.arrow.memory.RootAllocatorFactory),
 * Apache License 2.0. Trimmed: reads a plain max-bytes instead of SabotConfig.
 */
package org.fg.common.memory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates the root allocator, configuring the Netty allocator exactly once. */
public final class RootAllocatorFactory {

  private static final String IO_NETTY_ALLOCATOR_USE_CACHE_FOR_ALL_THREADS_PROPERTY =
      "io.netty.allocator.useCacheForAllThreads";

  private static final Logger LOGGER = LoggerFactory.getLogger(RootAllocatorFactory.class);

  /** Loaded lazily so the Netty system property is set before the default allocator is built. */
  private static final class InternalFactory {
    static {
      LOGGER.debug("Configuring Netty allocator");
      final String previousProperty =
          System.getProperty(IO_NETTY_ALLOCATOR_USE_CACHE_FOR_ALL_THREADS_PROPERTY);
      try {
        System.setProperty(IO_NETTY_ALLOCATOR_USE_CACHE_FOR_ALL_THREADS_PROPERTY, "false");
      } finally {
        if (previousProperty == null) {
          System.getProperties().remove(IO_NETTY_ALLOCATOR_USE_CACHE_FOR_ALL_THREADS_PROPERTY);
        } else {
          System.setProperty(
              IO_NETTY_ALLOCATOR_USE_CACHE_FOR_ALL_THREADS_PROPERTY, previousProperty);
        }
      }
    }

    static BufferAllocator newRoot(long maxAllocBytes) {
      return new RootAllocator(maxAllocBytes);
    }
  }

  private RootAllocatorFactory() {}

  public static BufferAllocator newRoot(long maxAllocBytes) {
    return InternalFactory.newRoot(maxAllocBytes);
  }
}
