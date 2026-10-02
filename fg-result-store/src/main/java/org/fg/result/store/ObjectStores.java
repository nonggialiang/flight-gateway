package org.fg.result.store;

import java.nio.file.Path;
import org.fg.common.config.GatewayConfig;

/** 从 GatewayConfig 构建 ObjectStoreService（D28：AWS SDK v2 + RangeCache 两级缓存）。 */
public final class ObjectStores {

  private ObjectStores() {}

  public static ObjectStoreService fromConfig(GatewayConfig config) {
    String diskDir = config.hasPath("fg.result.range-cache.disk-dir")
        ? config.getString("fg.result.range-cache.disk-dir").trim() : "";
    return new ObjectStoreService(
        config.getString("fg.result.s3.endpoint"),
        config.getString("fg.result.s3.access-key"),
        config.getString("fg.result.s3.secret-key"),
        config.hasPath("fg.result.s3.region") ? config.getString("fg.result.s3.region") : "us-east-1",
        config.getString("fg.result.bucket"),
        config.hasPath("fg.result.range-cache.max-memory")
            ? config.getDurationMs("fg.result.range-cache.max-memory") : 64L * 1024 * 1024,
        diskDir.isEmpty() ? null : Path.of(diskDir),
        config.hasPath("fg.result.range-cache.disk-max-bytes")
            ? Long.parseLong(config.getString("fg.result.range-cache.disk-max-bytes"))
            : 10L * 1024 * 1024 * 1024,
        config.hasPath("fg.result.s3.connect-timeout")
            ? config.getDurationMs("fg.result.s3.connect-timeout") : 10_000L,
        config.hasPath("fg.result.s3.read-timeout")
            ? config.getDurationMs("fg.result.s3.read-timeout") : 60_000L,
        60_000L);
  }
}
