package org.fg.result.store;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** D28 RangeCache 两级缓存：内存命中/磁盘命中(write-behind)/淘汰/关闭各层。 */
class RangeCacheTest {

  private static byte[] data(int len, byte seed) {
    byte[] b = new byte[len];
    java.util.Arrays.fill(b, seed);
    return b;
  }

  @Test
  void memoryHitAvoidsSecondLoad() throws Exception {
    AtomicInteger loads = new AtomicInteger();
    RangeCache cache = new RangeCache(1024 * 1024, null, 0, (k, o, l) -> {
      loads.incrementAndGet();
      return CompletableFuture.completedFuture(data(l, (byte) 1));
    });
    byte[] first = cache.read("k", 0, 100).get(5, TimeUnit.SECONDS);
    byte[] second = cache.read("k", 0, 100).get(5, TimeUnit.SECONDS);
    assertThat(first).hasSize(100).containsExactly(data(100, (byte) 1));
    assertThat(second).isEqualTo(first);
    assertThat(loads.get()).isEqualTo(1); // 命中零加载
  }

  @Test
  void memoryEvictionReloadsFromDiskNotLoader(@TempDir Path dir) throws Exception {
    AtomicInteger loads = new AtomicInteger();
    CountDownLatch written = new CountDownLatch(1);
    RangeCache cache = new RangeCache(64, dir, 1024 * 1024, (k, o, l) -> {
      loads.incrementAndGet();
      written.countDown();
      return CompletableFuture.completedFuture(data(l, (byte) 7));
    });
    // key A（100B）+ key B（100B）> 64B 上限 → A 被驱逐
    byte[] a = cache.read("A", 0, 100).get(5, TimeUnit.SECONDS);
    cache.read("B", 0, 100).get(5, TimeUnit.SECONDS);
    assertThat(written.await(5, TimeUnit.SECONDS)).isTrue();
    awaitDiskWrite(dir);
    // A 重读：内存已驱逐 → 磁盘命中（同字节），loader 不再调用
    byte[] aAgain = cache.read("A", 0, 100).get(5, TimeUnit.SECONDS);
    assertThat(aAgain).isEqualTo(a);
    assertThat(loads.get()).isEqualTo(2); // A、B 各一次，无第三次
    cache.close();
  }

  @Test
  void pureDiskTierWhenMemoryOff(@TempDir Path dir) throws Exception {
    AtomicInteger loads = new AtomicInteger();
    ConcurrentHashMap<String, byte[]> store = new ConcurrentHashMap<>();
    RangeCache cache = new RangeCache(0, dir, 1024 * 1024, (k, o, l) -> {
      loads.incrementAndGet();
      byte[] d = data(l, (byte) 9);
      store.put(k, d);
      return CompletableFuture.completedFuture(d);
    });
    cache.read("k", 16, 50).get(5, TimeUnit.SECONDS);
    awaitDiskWrite(dir);
    cache.read("k", 16, 50).get(5, TimeUnit.SECONDS); // 内存关 → 直接磁盘
    assertThat(loads.get()).isEqualTo(1);
    cache.close();
  }

  @Test
  void diskDisabledPureMemory() throws Exception {
    AtomicInteger loads = new AtomicInteger();
    RangeCache cache = new RangeCache(1024 * 1024, null, 0, (k, o, l) -> {
      loads.incrementAndGet();
      return CompletableFuture.completedFuture(data(l, (byte) 3));
    });
    cache.read("k", 0, 10).get(5, TimeUnit.SECONDS);
    cache.read("k", 0, 10).get(5, TimeUnit.SECONDS);
    assertThat(loads.get()).isEqualTo(1);
  }

  @Test
  void concurrentReadsSingleLoadWithoutMemory() throws Exception {
    // memory=0（无 Caffeine 去重）时并发 8 线程同 key 读 → inFlight 表保证 loader 恰好 1 次
    AtomicInteger loads = new AtomicInteger();
    java.util.concurrent.CountDownLatch startGate = new java.util.concurrent.CountDownLatch(1);
    RangeCache cache = new RangeCache(0, null, 0, (k, o, l) -> {
      loads.incrementAndGet();
      return CompletableFuture.supplyAsync(() -> {
        try {
          startGate.await(5, TimeUnit.SECONDS); // 制造加载窗口
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return data(l, (byte) 5);
      });
    });
    var futures = new java.util.ArrayList<CompletableFuture<byte[]>>();
    for (int i = 0; i < 8; i++) {
      futures.add(cache.read("K", 32, 64));
    }
    startGate.countDown();
    for (CompletableFuture<byte[]> f : futures) {
      assertThat(f.get(5, TimeUnit.SECONDS)).hasSize(64).containsExactly(data(64, (byte) 5));
    }
    assertThat(loads.get()).isEqualTo(1);
  }

  /** 写线程异步——轮询等最终文件落定（排除 .tmp 中间态，上限 5s）。 */
  private static void awaitDiskWrite(Path dir) throws Exception {
    long deadline = System.currentTimeMillis() + 5000;
    while (System.currentTimeMillis() < deadline) {
      boolean done = Files.walk(dir)
          .filter(Files::isRegularFile)
          .anyMatch(f -> !f.getFileName().toString().endsWith(".tmp"));
      if (done) {
        return;
      }
      Thread.sleep(50);
    }
    throw new AssertionError("disk write not visible in 5s");
  }
}
