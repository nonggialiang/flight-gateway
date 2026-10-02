package org.fg.result.store;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 两级 Range 读缓存（D28 改造，用户设计：Caffeine 内存索引 + 异步磁盘层）。
 *
 * <p><b>内存层</b>：Caffeine {@link AsyncCache}，key=(objectKey, offset, length)，weigher =
 * 字节数，{@code maximumWeight} = 配置内存上限——页/bidx 重复翻页命中零网络。
 *
 * <p><b>磁盘层（write-behind）</b>：S3 加载成功后把 byte[] 提交专门线程池经
 * {@link AsynchronousFileChannel} 异步刷盘（不阻塞读路径）；内存 miss 时先查磁盘
 * （同 key 命中即零 S3）。目录布局 {@code dir/<keyHash>/<offset>-<length>.bin}；总量
 * 超 {@code diskMaxBytes} 时按 lastModified 淘汰最旧文件（加载路径同步检查，代价小：
 * 每文件一个 stat）。快照不可变（scroll 快照），两层都无需失效协议。
 *
 * <p>两级都可独立关闭：maxMemoryBytes=0 → 纯直读；diskDir 空 → 无磁盘层。
 */
final class RangeCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(RangeCache.class);

  record RangeKey(String objectKey, long offset, int length) {}

  /** S3 ranged 加载器（ObjectStoreService 注入，异步）。 */
  interface Loader {
    CompletableFuture<byte[]> load(String objectKey, long offset, int length);
  }

  private final long maxMemoryBytes;
  private final Path diskDir;
  private final long diskMaxBytes;
  private final Loader loader;

  private final AsyncCache<RangeKey, byte[]> memory;
  private final ExecutorService diskWriter;
  private final AtomicLong diskBytes = new AtomicLong();
  private volatile boolean diskSizeKnown;

  RangeCache(long maxMemoryBytes, Path diskDir, long diskMaxBytes, Loader loader) {
    this.maxMemoryBytes = maxMemoryBytes;
    this.diskDir = diskDir;
    this.diskMaxBytes = diskMaxBytes;
    this.loader = loader;

    if (diskDir != null) {
      this.diskWriter = Executors.newFixedThreadPool(1, r -> {
        Thread t = new Thread(r, "fg-range-disk-writer");
        t.setDaemon(true);
        return t;
      });
    } else {
      this.diskWriter = null;
    }

    if (maxMemoryBytes > 0) {
      this.memory = Caffeine.newBuilder()
          .maximumWeight(maxMemoryBytes)
          .weigher((RangeKey k, byte[] v) -> v.length)
          .removalListener((RemovalListener<RangeKey, byte[]>) (key, value, cause) ->
              LOGGER.debug("range evicted from memory: {} cause={}", key, cause))
          .buildAsync();
    } else {
      this.memory = null;
    }
  }

  /** 读 [offset, offset+length)：内存 → 磁盘 → S3（S3 成功后异步落盘）。 */
  CompletableFuture<byte[]> read(String objectKey, long offset, int length) {
    if (memory != null) {
      return memory.get(new RangeKey(objectKey, offset, length),
          (key, exec) -> loadFromDiskOrRemote(key));
    }
    return loadFromDiskOrRemote(new RangeKey(objectKey, offset, length));
  }

  private CompletableFuture<byte[]> loadFromDiskOrRemote(RangeKey key) {
    byte[] fromDisk = readDisk(key);
    if (fromDisk != null) {
      return CompletableFuture.completedFuture(fromDisk);
    }
    return loader.load(key.objectKey(), key.offset(), key.length())
        .thenApply(bytes -> {
          writeDiskAsync(key, bytes);
          return bytes;
        });
  }

  // ------------------------------------------------------------- 磁盘层

  private Path fileFor(RangeKey key) {
    return diskDir.resolve(HexFormat.of().formatHex(
            key.objectKey().getBytes(java.nio.charset.StandardCharsets.UTF_8)))
        .resolve(key.offset() + "-" + key.length() + ".bin");
  }

  private byte[] readDisk(RangeKey key) {
    if (diskDir == null) {
      return null;
    }
    Path file = fileFor(key);
    try {
      if (!Files.isRegularFile(file)) {
        return null;
      }
      byte[] data = Files.readAllBytes(file);
      if (data.length != key.length()) {
        return null; // 尺寸不符（半写）：视作 miss 重拉
      }
      return data;
    } catch (IOException e) {
      return null;
    }
  }

  /** write-behind：S3 数据异步刷盘 + 总量守卫（超限按 lastModified 淘汰最旧）。 */
  private void writeDiskAsync(RangeKey key, byte[] bytes) {
    if (diskWriter == null) {
      return;
    }
    diskWriter.execute(() -> {
      try {
        Path file = fileFor(key);
        Files.createDirectories(file.getParent());
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        try (AsynchronousFileChannel ch = AsynchronousFileChannel.open(tmp,
            StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
          java.util.concurrent.Future<Integer> f = ch.write(ByteBuffer.wrap(bytes), 0);
          f.get();
        }
        Files.move(tmp, file,
            java.nio.file.StandardCopyOption.REPLACE_EXISTING,
            java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        diskBytes.addAndGet(bytes.length);
        enforceDiskBudget();
      } catch (Exception e) {
        LOGGER.debug("range disk write failed: {} ({})", key, e.toString());
      }
    });
  }

  /** 单写线程串行调用；首次惰性统计目录现有量；超限按 lastModified 升序删除至回预算内。 */
  private synchronized void enforceDiskBudget() throws IOException {
    if (!diskSizeKnown) {
      diskBytes.set(dirSize(diskDir));
      diskSizeKnown = true;
    }
    if (diskMaxBytes <= 0 || diskBytes.get() <= diskMaxBytes) {
      return;
    }
    try (var files = Files.walk(diskDir)) {
      files.filter(Files::isRegularFile)
          .map(Path::toFile)
          .sorted(Comparator.comparingLong(java.io.File::lastModified))
          .forEach(file -> {
            if (diskBytes.get() <= diskMaxBytes) {
              return;
            }
            long size = file.length();
            if (file.delete()) {
              diskBytes.addAndGet(-size);
              LOGGER.debug("range disk evicted: {}", file);
            }
          });
    }
  }

  private long dirSize(Path dir) throws IOException {
    try (var files = Files.walk(dir)) {
      return files.filter(Files::isRegularFile).mapToLong(p -> {
        try {
          return Files.size(p);
        } catch (IOException e) {
          return 0L;
        }
      }).sum();
    }
  }

  void close() {
    if (diskWriter != null) {
      diskWriter.shutdown();
      try {
        // 有界等待 write-behind 落定（测试 TempDir 清理与写线程的竞态修复）
        diskWriter.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  long maxMemoryBytes() {
    return maxMemoryBytes;
  }
}
