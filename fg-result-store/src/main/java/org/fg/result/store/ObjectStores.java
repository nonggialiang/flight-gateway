package org.fg.result.store;

import java.nio.file.Path;
import org.fg.common.config.GatewayConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 从 GatewayConfig 构建 ObjectStoreService（D28：AWS SDK v2 + RangeCache 两级缓存）。 */
public final class ObjectStores {

  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStores.class);

  private ObjectStores() {}

  public static ObjectStoreService fromConfig(GatewayConfig config) {
    String diskDir = config.hasPath("fg.result.range-cache.disk-dir")
        ? config.getString("fg.result.range-cache.disk-dir").trim() : "";
    // 字节语义（D30 后补修正）：max-memory 原经 getDurationMs 解析——"64m" 被当成
    // 64 分钟的毫秒数（≈3.8MB）而非 64MiB；getBytes 才是本意（0=关内存层）
    long maxMemoryBytes = config.hasPath("fg.result.range-cache.max-memory")
        ? config.getBytes("fg.result.range-cache.max-memory") : 64L * 1024 * 1024;
    long diskMaxBytes = config.hasPath("fg.result.range-cache.disk-max-bytes")
        ? config.getBytes("fg.result.range-cache.disk-max-bytes") : 10L * 1024 * 1024 * 1024;
    long callTimeoutMs = config.hasPath("fg.result.s3.call-timeout")
        ? config.getDurationMs("fg.result.s3.call-timeout") : 600_000L;
    LOGGER.info(
        "range cache: memoryTier={}B diskDir={} diskBudget={}B s3CallTimeout={}ms",
        maxMemoryBytes, diskDir.isEmpty() ? "(off)" : diskDir, diskMaxBytes, callTimeoutMs);
    return new ObjectStoreService(
        config.getString("fg.result.s3.endpoint"),
        config.getString("fg.result.s3.access-key"),
        config.getString("fg.result.s3.secret-key"),
        config.hasPath("fg.result.s3.region") ? config.getString("fg.result.s3.region") : "us-east-1",
        config.getString("fg.result.bucket"),
        maxMemoryBytes,
        diskDir.isEmpty() ? null : Path.of(diskDir),
        diskMaxBytes,
        config.hasPath("fg.result.s3.connect-timeout")
            ? config.getDurationMs("fg.result.s3.connect-timeout") : 10_000L,
        config.hasPath("fg.result.s3.read-timeout")
            ? config.getDurationMs("fg.result.s3.read-timeout") : 60_000L,
        callTimeoutMs);
  }
}
