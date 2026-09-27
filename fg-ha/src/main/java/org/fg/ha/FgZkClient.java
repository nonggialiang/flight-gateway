package org.fg.ha;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

/**
 * Curator 薄封装（D22）——gateway（发现/锁/守护删除）与 engine（注册/DeReg watch/LOST 宽限）
 * 共用的唯一 ZK 访问点。发现侧刻意<strong>不做</strong> watcher 缓存：CONNECTION share level
 * 使 space 数无界（每会话一个），缓存会泄漏；每 RPC 一次 children roundtrip 与既有的 admin
 * 探测同量级，换取"死引擎在下一 RPC 即被察觉"的直白正确性。watch 只用在语义要害处
 * （引擎自身节点 DeReg + ZK LOST）。
 *
 * <p>拉起互斥（Kyuubi EngineRef 三段式）：冷启动经 {@link #tryLock} 在锁内 double-check
 * （四 share level 统一；CONNECTION 的锁粒度为用户级——逐会话锁路径是持久 znode，会话数
 * 无界即泄漏）；{@link #deregisterIfStale} 的锁内 host:port 匹配守卫防止把顶替上来的
 * 新引擎 znode 删掉。
 */
public final class FgZkClient implements AutoCloseable {

  private static final long ZNODE_CREATE_TIMEOUT_MS = 60_000L;

  private static final ScheduledExecutorService CONNECTION_CHECKER =
      Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "fg-zk-connection-checker");
        t.setDaemon(true);
        return t;
      });

  private final CuratorFramework client;
  private final String namespace;

  public FgZkClient(String connectString, String namespace) {
    this(connectString, namespace, 60_000, 15_000);
  }

  public FgZkClient(String connectString, String namespace, int sessionTimeoutMs, int connectionTimeoutMs) {
    this.namespace = namespace;
    this.client = CuratorFrameworkFactory.builder()
        .connectString(connectString)
        .sessionTimeoutMs(sessionTimeoutMs)
        .connectionTimeoutMs(connectionTimeoutMs)
        .retryPolicy(new RetryForever(1000))
        .build();
  }

  public void start() {
    client.start();
  }

  @Override
  public void close() {
    client.close();
  }

  /** 命名空间（engineRoot 构造用）。 */
  public String namespace() {
    return namespace;
  }

  // ------------------------------------------------------------- 发现（读穿）

  /** 列出 space 内引擎（按 sequence 升序；末位 = 最新）。space 不存在 = 空。 */
  public List<EngineNode> listEngines(String engineSpace) {
    try {
      List<String> children = client.getChildren().forPath(engineSpace);
      List<EngineNode> nodes = new ArrayList<>(children.size());
      for (String child : children) {
        byte[] data = client.getData().forPath(engineSpace + "/" + child);
        nodes.add(EngineNode.parse(child, data));
      }
      nodes.sort(Comparator.comparingLong(EngineNode::sequence));
      return nodes;
    } catch (KeeperException.NoNodeException e) {
      return List.of();
    } catch (Exception e) {
      throw new IllegalStateException("zk listEngines failed for " + engineSpace, e);
    }
  }

  /** 按 refId 定位引擎（拉起轮询用）。 */
  public Optional<EngineNode> engineByRefId(String engineSpace, String refId) {
    return listEngines(engineSpace).stream()
        .filter(n -> n.refId() != null && n.refId().equals(refId))
        .findFirst();
  }

  // ------------------------------------------------------------- 锁

  /**
   * 分布式互斥（InterProcessSemaphoreMutex）。超时抛 {@link IllegalStateException}
   * （他方拉起卡死时 fast-fail，Kyuubi 同构）。
   */
  public <T> T tryLock(String lockPath, long timeoutMs, Callable<T> body) throws Exception {
    InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(client, lockPath);
    boolean acquired = false;
    try {
      acquired = lock.acquire(timeoutMs, TimeUnit.MILLISECONDS);
      if (!acquired) {
        throw new IllegalStateException(
            "Timeout acquiring engine lock " + lockPath + " after " + timeoutMs + "ms");
      }
      return body.call();
    } finally {
      if (acquired) {
        try {
          lock.release();
        } catch (Exception ignored) {
          // 释放失败：锁随会话过期自愈
        }
      }
    }
  }

  // ------------------------------------------------------------- 注册（引擎侧）

  /**
   * 注册本引擎为 space 下 EPHEMERAL_SEQUENTIAL 节点（PersistentNode：会话重连后自动重建）。
   * 返回句柄 close() 即注销。
   */
  public Registration registerEphemeral(String engineSpace, String host, int connectPort, int adminPort,
      String version, String refId) {
    try {
      try {
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(engineSpace);
      } catch (KeeperException.NodeExistsException ignored) {
        // space 已存在
      }
      String prefix = engineSpace + "/" + EngineNode.znodePrefix(host, connectPort, adminPort, version, refId);
      byte[] data = EngineNode.instance(host, connectPort).getBytes(StandardCharsets.UTF_8);
      PersistentNode node = new PersistentNode(client, CreateMode.EPHEMERAL_SEQUENTIAL, false, prefix, data);
      try {
        node.start();
        if (!node.waitForInitialCreate(ZNODE_CREATE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          throw new IllegalStateException(
              "Timed out creating engine znode under " + engineSpace);
        }
      } catch (Exception e) {
        try {
          node.close();
        } catch (Exception ignored) {
        }
        throw e;
      }
      String actualPath = node.getActualPath();
      return new Registration() {
        @Override
        public String path() {
          return actualPath;
        }

        @Override
        public void close() {
          try {
            node.close();
          } catch (Exception ignored) {
          }
        }
      };
    } catch (Exception e) {
      throw new IllegalStateException("Failed to register engine under " + engineSpace, e);
    }
  }

  /** 注册句柄：path() = 实际 znode 路径（watch 用）；close() = 注销。 */
  public interface Registration extends AutoCloseable {
    String path();

    @Override
    void close();
  }

  // ------------------------------------------------------------- 注销守卫（网关侧恢复）

  /**
   * 守卫式注销（恢复路径用）：仅当 space <b>最新</b> znode 的 host:port 与陈旧引擎一致时删除
   * ——防止把已顶替的新引擎删掉。注销在 lockPath 锁内执行（与拉起同款互斥；CONNECTION
   * 传用户级锁路径）。best-effort：任何异常返回 false。
   */
  public boolean deregisterIfStale(String engineSpace, String lockPath, String host, int connectPort,
      long lockTimeoutMs) {
    Callable<Boolean> guarded = () -> {
      List<EngineNode> nodes = listEngines(engineSpace);
      if (nodes.isEmpty()) {
        return false;
      }
      EngineNode newest = nodes.get(nodes.size() - 1);
      if (newest.host().equals(host) && newest.connectPort() == connectPort) {
        client.delete().forPath(engineSpace + "/" + newest.znodeName());
        deleteIfEmpty(engineSpace);
        return true;
      }
      return false;
    };
    try {
      return lockPath != null ? tryLock(lockPath, lockTimeoutMs, guarded) : guarded.call();
    } catch (Exception e) {
      return false;
    }
  }

  /** best-effort 删除空目录（CONNECTION space 清理；非空/不存在静默）。 */
  public void deleteIfEmpty(String path) {
    try {
      List<String> children = client.getChildren().forPath(path);
      if (children == null || children.isEmpty()) {
        client.delete().forPath(path);
      }
    } catch (Exception ignored) {
      // best-effort
    }
  }

  // ------------------------------------------------------------- watch（引擎侧）

  /**
   * 监视自身节点被删（网关/运维摘流）→ 回调（引擎优雅停）。NodeDataChanged 时重臂。
   * 节点已不存在或臂表失败时立即回调（fail-safe：宁可误停不可僵留）。
   */
  public void watchForDelete(String path, Runnable onDeleted) {
    try {
      Stat stat = client.checkExists().usingWatcher((Watcher) event -> {
        Watcher.Event.EventType type = event.getType();
        if (type == Watcher.Event.EventType.NodeDeleted) {
          onDeleted.run();
        } else if (type == Watcher.Event.EventType.NodeDataChanged) {
          watchForDelete(path, onDeleted);
        }
      }).forPath(path);
      if (stat == null) {
        onDeleted.run();
      }
    } catch (Exception e) {
      onDeleted.run();
    }
  }

  /**
   * ZK 连接 LOST 宽限（Kyuubi monitorState 范式）：LOST 后 graceMs 内未恢复即回调
   * （引擎侧自杀——ephemeral 已随会话消失，继续服务会脱离发现面）。
   */
  public void onConnectionLost(long graceMs, Runnable onLost) {
    AtomicBoolean connected = new AtomicBoolean(false);
    client.getConnectionStateListenable().addListener((ConnectionStateListener) (c, state) -> {
      if (state == ConnectionState.CONNECTED || state == ConnectionState.RECONNECTED) {
        connected.set(true);
      } else if (state == ConnectionState.LOST) {
        connected.set(false);
        CONNECTION_CHECKER.schedule(() -> {
          if (!connected.get()) {
            onLost.run();
          }
        }, graceMs, TimeUnit.MILLISECONDS);
      }
    });
  }

  // ------------------------------------------------------------- 测试支撑

  /** 直删 znode（IT 用）。 */
  void delete(String path) throws Exception {
    client.delete().forPath(path);
  }

  /** 裸 ephemeral 注册（IT 用：模拟已死引擎残留的 znode——无 PersistentNode 属主，删后不重建）。 */
  void createRawEphemeral(String path, byte[] data) throws Exception {
    client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, data);
  }
}
