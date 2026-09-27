package org.fg.ha;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * 真 ZooKeeper（testcontainers zookeeper:3.9）集成验证：注册→发现→refId 定位→
 * 注销守卫（stale/fresh 顶替）→锁互斥→锁超时 fast-fail。
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FgZkClientIT {

  @Container
  private static final GenericContainer<?> ZK =
      new GenericContainer<>(DockerImageName.parse("zookeeper:3.9")).withExposedPorts(2181);

  private static FgZkClient client;

  private static final String SPACE = "/fg-it_v1_USER_spark/alice/default";
  private static final String LOCK = "/fg-it_v1_USER_spark_lock/alice/default";

  @BeforeAll
  static void startClient() {
    client = new FgZkClient("localhost:" + ZK.getMappedPort(2181), "fg-it", 10_000, 10_000);
    client.start();
  }

  @AfterAll
  static void stopClient() {
    if (client != null) {
      client.close();
    }
  }

  private static void awaitSize(int expected) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 15_000;
    while (client.listEngines(SPACE).size() != expected) {
      assertThat(System.currentTimeMillis() < deadline).as("znode count -> %d", expected).isTrue();
      Thread.sleep(200);
    }
  }

  @Test
  @Order(1)
  void registerThenListAndFindByRefId() throws Exception {
    assertThat(client.listEngines(SPACE)).isEmpty();

    FgZkClient.Registration a = client.registerEphemeral(SPACE, "hostA", 16001, 17001, null, "ref-a");
    FgZkClient.Registration b = client.registerEphemeral(SPACE, "hostB", 16002, 17002, null, "ref-b");

    List<EngineNode> nodes = client.listEngines(SPACE);
    assertThat(nodes).hasSize(2);
    assertThat(nodes).extracting(EngineNode::refId).containsExactly("ref-a", "ref-b"); // sequence 升序

    EngineNode byRef = client.engineByRefId(SPACE, "ref-b").orElseThrow();
    assertThat(byRef.host()).isEqualTo("hostB");
    assertThat(byRef.connectPort()).isEqualTo(16002);
    assertThat(byRef.adminPort()).isEqualTo(17002);

    b.close();
    awaitSize(1);
    assertThat(client.listEngines(SPACE).get(0).refId()).isEqualTo("ref-a");
    a.close();
    awaitSize(0);
  }

  @Test
  @Order(2)
  void deregisterGuardLeavesFreshReplacementAlone() throws Exception {
    // 在活引擎 A（PersistentNode 属主）
    FgZkClient.Registration live = client.registerEphemeral(SPACE, "hostA", 16001, 17001, null, "ref-a");
    EngineNode liveNode = EngineNode.parse(pathName(live.path()), null);
    // 已死引擎 B 的残留 znode（裸 ephemeral，无属主不重建；sequence 压过 A = 最新节点）
    String deadName = EngineNode.znodePrefix("hostB", 16002, 17002, null, "ref-b")
        + String.format("%010d", liveNode.sequence() + 1);
    client.createRawEphemeral(SPACE + "/" + deadName,
        EngineNode.instance("hostB", 16002).getBytes(StandardCharsets.UTF_8));
    awaitSize(2);

    // 守卫 no-op：B（最新）与 A host:port 不符——对 A 的注销不删任何节点
    assertThat(client.deregisterIfStale(SPACE, LOCK, "hostA", 16001, 5_000)).isFalse();
    assertThat(client.listEngines(SPACE)).hasSize(2);

    // 守卫命中：B 是最新节点且 host:port 匹配——删除（恢复路径的正常情形）
    assertThat(client.deregisterIfStale(SPACE, LOCK, "hostB", 16002, 5_000)).isTrue();
    List<EngineNode> after = client.listEngines(SPACE);
    assertThat(after).hasSize(1);
    assertThat(after.get(0).host()).isEqualTo("hostA");

    live.close();
    awaitSize(0);
  }

  private static String pathName(String path) {
    return path.substring(path.lastIndexOf('/') + 1);
  }

  @Test
  @Order(3)
  void lockMutuallyExcludes() throws Exception {
    AtomicInteger inside = new AtomicInteger();
    AtomicInteger maxConcurrent = new AtomicInteger();
    CountDownLatch bothDone = new CountDownLatch(2);
    for (int i = 0; i < 2; i++) {
      int id = i;
      new Thread(() -> {
        try {
          client.tryLock(LOCK, 15_000, () -> {
            int now = inside.incrementAndGet();
            maxConcurrent.accumulateAndGet(now, Math::max);
            Thread.sleep(300); // 制造重叠窗口
            inside.decrementAndGet();
            return null;
          });
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          bothDone.countDown();
        }
      }, "lock-test-" + id).start();
    }
    assertThat(bothDone.await(30, TimeUnit.SECONDS)).isTrue();
    assertThat(maxConcurrent.get()).isEqualTo(1);
  }

  @Test
  @Order(4)
  void lockTimeoutFastFails() throws Exception {
    CountDownLatch held = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Thread holder = new Thread(() -> {
      try {
        client.tryLock(LOCK, 15_000, () -> {
          held.countDown();
          release.await();
          return null;
        });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, "lock-holder");
    holder.start();
    assertThat(held.await(15, TimeUnit.SECONDS)).isTrue();

    long start = System.currentTimeMillis();
    assertThatThrownBy(() -> client.tryLock(LOCK, 200, () -> null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Timeout acquiring engine lock");
    assertThat(System.currentTimeMillis() - start).isLessThan(5_000);
    release.countDown();
    holder.join(15_000);
  }
}
