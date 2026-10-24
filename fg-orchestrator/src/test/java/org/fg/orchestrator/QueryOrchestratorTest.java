package org.fg.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.fg.common.config.GatewayConfig;
import org.fg.orchestrator.store.OperationRow;
import org.fg.orchestrator.store.OperationStoreDao;
import org.fg.orchestrator.store.SessionRegistryDao;
import org.fg.result.manifest.ResultManifest;
import org.fg.result.store.ObjectStoreService;
import org.fg.spi.EngineExecutionHandle;
import org.fg.spi.EngineSession;
import org.fg.spi.ExecutionOutcome;
import org.fg.spi.SqlEngine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

/**
 * D36：poll 行级 deadline + attach future 唤醒；maybeAttach 去租约（cooldown/grace）。
 * 全 mock（dao/engine/objects），controlExecutor 用真池——attach 是 supplyAsync。
 */
class QueryOrchestratorTest {

  private static final String[] KEYS = {
    "fg.poll.db.interval", "fg.query.timeout", "fg.attach.retry.cooldown", "fg.attach.orphan-grace"
  };

  private OperationStoreDao dao;
  private SqlEngine engine;
  private ObjectStoreService objects;
  private SessionRegistryDao sessions;
  private QueryOrchestrator orchestrator;
  private ExecutorService executor;
  private GatewayConfig config;
  private final AtomicReference<OperationRow> rowState = new AtomicReference<>();
  private final AtomicInteger attachCalls = new AtomicInteger();
  private CompletableFuture<ExecutionOutcome> attachFuture;

  @BeforeAll
  @AfterAll
  static void resetProps() {
    for (String k : KEYS) {
      System.clearProperty(k);
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    System.setProperty("fg.poll.db.interval", "10s"); // tick 设大：任何 <10s 的返回都证明是 future 唤醒
    System.setProperty("fg.query.timeout", "300s");
    System.setProperty("fg.attach.retry.cooldown", "50ms");
    System.setProperty("fg.attach.orphan-grace", "500ms");
    config = GatewayConfig.create();

    dao = mock(OperationStoreDao.class);
    engine = mock(SqlEngine.class);
    objects = mock(ObjectStoreService.class);
    sessions = mock(SessionRegistryDao.class);
    executor = Executors.newCachedThreadPool();

    // sessionFor:bornOrGet → ACTIVE 行(attach 路径只取化身,不做生命周期校验)
    SessionRegistryDao.SessionRow sessionRow =
        new SessionRegistryDao.SessionRow(
            "s-1", "alice", "spark", null, List.of(),
            SessionRegistryDao.Status.ACTIVE, null);
    when(sessions.bornOrGet(anyString(), anyString(), anyString()))
        .thenReturn(new SessionRegistryDao.Born(sessionRow, false));
    when(engine.openSession(any())).thenReturn(mock(EngineSession.class));

    orchestrator = new QueryOrchestrator(dao, sessions, engine, objects, config, executor);

    when(dao.get(anyString())).thenAnswer(this::currentRow);
    when(dao.casComplete(anyString()))
        .thenAnswer(inv -> migrate(OperationRow.Status.COMPLETED));
    when(dao.casFail(anyString(), anyString()))
        .thenAnswer(
            inv -> {
              rowState.set(rowState.get().status(OperationRow.Status.FAILED).error(inv.getArgument(1)));
              return true;
            });
    when(dao.casCancel(anyString())).thenAnswer(inv -> migrate(OperationRow.Status.CANCELLED));
    when(objects.manifestExists(anyString())).thenReturn(false);
    when(engine.attach(any(), any()))
        .thenAnswer(
            inv -> {
              attachCalls.incrementAndGet();
              return attachFuture; // 每个用例替换
            });
    when(engine.openSession(any())).thenReturn(mock(EngineSession.class));
  }

  @AfterEach
  void tearDown() {
    executor.shutdownNow();
    for (String k : KEYS) { // 用例自定义值不外溢到后续用例
      System.clearProperty(k);
    }
  }

  private Object currentRow(InvocationOnMock inv) {
    return Optional.ofNullable(rowState.get());
  }

  private boolean migrate(OperationRow.Status status) {
    OperationRow row = rowState.get();
    if (row == null || row.status().terminal()) {
      return false;
    }
    rowState.set(row.status(status));
    return true;
  }

  private OperationRow runningRow(OffsetDateTime createdAt, String opId) {
    return new OperationRow()
        .queryId("q-1")
        .sessionRef("s-1")
        .sqlHash("h")
        .user("alice")
        .sqlText("SELECT 1")
        .resultKeyPrefix("results/spark/alice/q-1")
        .mode(OperationRow.Mode.RELAY)
        .status(OperationRow.Status.RUNNING)
        .connectOperationId(opId)
        .engineRef("spark")
        .createdAt(createdAt);
  }

  /** 1：attach future 完成 → casComplete → poll 毫秒级醒来（tick=10s 下 <2s 即证 future 唤醒）。 */
  @Test
  void pollWakesOnAttachFutureCompletion() throws Exception {
    rowState.set(runningRow(OffsetDateTime.now(), "spark:op-1"));
    attachFuture = new CompletableFuture<>(); // 不预完成：先让 poll 挂上再触发

    AtomicReference<QueryOrchestrator.PollOutcome> result = new AtomicReference<>();
    Thread poller =
        new Thread(
            () -> {
              try {
                result.set(orchestrator.poll("q-1"));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    poller.start();
    awaitAttachEstablished();
    long t0 = System.nanoTime();
    attachFuture.complete(ExecutionOutcome.completed());
    poller.join(5000);

    assertThat(result.get()).isNotNull();
    assertThat(result.get().done()).isTrue();
    assertThat(result.get().row().status()).isEqualTo(OperationRow.Status.COMPLETED);
    long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
    assertThat(ms).as("future 唤醒应远小于 tick(10s)").isLessThan(2000);
    verify(dao, atLeastOnce()).casComplete("q-1");
  }

  private void awaitAttachEstablished() throws InterruptedException {
    long deadline = System.currentTimeMillis() + 5000;
    while (attachCalls.get() == 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
    }
    assertThat(attachCalls.get()).as("attach 应已建立").isGreaterThanOrEqualTo(1);
    Thread.sleep(100); // 让 poll 线程进入 attach.get 等待
  }

  /** 2：UNKNOWN（传输类）→ cooldown 生效期内 attach 恰 1 次；不 casFail（sweeper 兜底）。 */
  @Test
  void transportUnknownEntersCooldownWithoutFailing() throws Exception {
    System.setProperty("fg.query.timeout", "300ms");
    System.setProperty("fg.attach.retry.cooldown", "10s"); // 冷却 > 窗口：整个窗口内 attach 恰 1 次
    config = GatewayConfig.create();
    orchestrator =
        new QueryOrchestrator(
            dao, sessions, engine, objects, config, executor);

    rowState.set(runningRow(OffsetDateTime.now(), "spark:op-2"));
    attachFuture = new CompletableFuture<>();
    attachFuture.complete(
        ExecutionOutcome.unknown("reattach transport error: UNAVAILABLE: conn refused"));

    QueryOrchestrator.PollOutcome out = orchestrator.poll("q-1");
    assertThat(out.done()).as("行级 deadline 到点，reconcile 无 manifest → not-done").isFalse();
    assertThat(attachCalls.get())
        .as("cooldown(10s) > 窗口(300ms)：整个窗口内 attach 恰 1 次")
        .isEqualTo(1);
    verify(dao, never()).casFail(anyString(), anyString());
  }

  /**
   * 3：本地 attach 判 COMPLETED 但行已被"他实例"迁 CANCELLED → poll 返回 CANCELLED
   * （响应以重读行为准——casComplete 撞终态行 no-op）。
   */
  @Test
  void responseFollowsDbRowNotLocalOutcome() throws Exception {
    rowState.set(runningRow(OffsetDateTime.now(), "spark:op-3"));
    attachFuture = new CompletableFuture<>();

    AtomicReference<QueryOrchestrator.PollOutcome> result = new AtomicReference<>();
    Thread poller =
        new Thread(
            () -> {
              try {
                result.set(orchestrator.poll("q-1"));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    poller.start();
    awaitAttachEstablished();
    // 他实例先赢：行迁 CANCELLED，本地 attach 随后以 completed 完成
    rowState.set(rowState.get().status(OperationRow.Status.CANCELLED));
    attachFuture.complete(ExecutionOutcome.completed());
    poller.join(5000);

    assertThat(result.get()).isNotNull();
    assertThat(result.get().done()).isTrue();
    assertThat(result.get().row().status())
        .as("以行为准，非本地 outcome")
        .isEqualTo(OperationRow.Status.CANCELLED);
  }

  /** 4：行级 deadline（created_at + query.timeout）到点仍 RUNNING → not-done。 */
  @Test
  void rowLevelDeadlineReturnsNotDone() throws Exception {
    System.setProperty("fg.query.timeout", "200ms");
    config = GatewayConfig.create();
    orchestrator =
        new QueryOrchestrator(
            dao, sessions, engine, objects, config, executor);
    rowState.set(runningRow(OffsetDateTime.now().minusSeconds(10), "spark:op-4"));
    attachFuture = new CompletableFuture<>(); // 永不完成：引擎 wedge

    QueryOrchestrator.PollOutcome out = orchestrator.poll("q-1");
    assertThat(out.done()).isFalse();
  }

  /** 5：COMMAND 行永不 attach。 */
  @Test
  void commandRowNeverAttaches() throws Exception {
    System.setProperty("fg.query.timeout", "200ms");
    config = GatewayConfig.create();
    orchestrator =
        new QueryOrchestrator(
            dao, sessions, engine, objects, config, executor);
    rowState.set(runningRow(OffsetDateTime.now(), null).kind(OperationRow.Kind.COMMAND));

    QueryOrchestrator.PollOutcome out = orchestrator.poll("q-1");
    assertThat(out.done()).isFalse();
    verify(engine, never()).attach(any(), any());
  }

  /** 6：双 poll 并发 → attach 恰一次（putIfAbsent 去重）。 */
  @Test
  void concurrentPollsShareSingleAttach() throws Exception {
    rowState.set(runningRow(OffsetDateTime.now(), "spark:op-6"));
    attachFuture = new CompletableFuture<>();

    var t1 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return orchestrator.poll("q-1");
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    var t2 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return orchestrator.poll("q-1");
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    Thread.sleep(300); // 两个 poll 都已挂上
    attachFuture.complete(ExecutionOutcome.completed());

    assertThat(t1.get(5, TimeUnit.SECONDS).done()).isTrue();
    assertThat(t2.get(5, TimeUnit.SECONDS).done()).isTrue();
    assertThat(attachCalls.get()).isEqualTo(1);
  }

  /** 7：cancelled → InterruptedException。 */
  @Test
  void cancelledPollThrows() {
    rowState.set(runningRow(OffsetDateTime.now(), "spark:op-7"));
    attachFuture = new CompletableFuture<>();
    assertThatThrownBy(() -> orchestrator.poll("q-1", () -> true))
        .isInstanceOf(InterruptedException.class);
  }

  /**
   * 8：not-found（抢跑）grace 语义——行龄 < grace：cooldown 重试不误杀；行龄 ≥ grace 且无
   * manifest：casFail("never submitted") 快败。
   */
  @Test
  void notFoundRespectsOrphanGrace() throws Exception {
    // 8a：新鲜行（createdAt=now，grace=500ms）——不判死
    System.setProperty("fg.query.timeout", "250ms");
    config = GatewayConfig.create();
    orchestrator =
        new QueryOrchestrator(
            dao, sessions, engine, objects, config, executor);
    rowState.set(runningRow(OffsetDateTime.now(), "spark:op-8a"));
    attachFuture = new CompletableFuture<>();
    attachFuture.complete(ExecutionOutcome.unknown("operation not found: NOT_FOUND: no op"));

    QueryOrchestrator.PollOutcome out = orchestrator.poll("q-1");
    assertThat(out.done()).isFalse();
    verify(dao, never()).casFail(anyString(), anyString());

    // 8b：老行（createdAt=now-10s > grace 500ms）——快败 FAILED。快败经 attach 善后异步
    // 落行：首次 poll 到行级 deadline（早已过）先 not-done 触发 attach，等善后 casFail
    // 落行后再次 poll 见 FAILED 终态。
    rowState.set(runningRow(OffsetDateTime.now().minusSeconds(10), "spark:op-8b"));
    attachCalls.set(0);
    // 行级 deadline 已过：每次 poll 立即 not-done，但都会经 maybeAttach 重试（8a 的冷却
    // 残留可能压住首次窗口）——循环 poll 直到善后 casFail 落行
    long waitUntil = System.currentTimeMillis() + 3000;
    QueryOrchestrator.PollOutcome out2 = orchestrator.poll("q-1");
    while (!out2.done() && System.currentTimeMillis() < waitUntil) {
      Thread.sleep(20);
      out2 = orchestrator.poll("q-1");
    }
    assertThat(out2.done()).isTrue();
    assertThat(attachCalls.get()).isGreaterThanOrEqualTo(1);
    assertThat(out2.row().status()).isEqualTo(OperationRow.Status.FAILED);
    assertThat(out2.row().error()).contains("never submitted");
  }

  /** 9：pollOnce（PART/页票）：终态即返、RUNNING 不等待不建 attach。 */
  @Test
  void pollOnceNeverAttachesNorWaits() throws Exception {
    rowState.set(runningRow(OffsetDateTime.now(), "spark:op-9"));
    QueryOrchestrator.PollOutcome notDone = orchestrator.pollOnce("q-1");
    assertThat(notDone.done()).isFalse();
    verify(engine, never()).attach(any(), any());

    rowState.set(runningRow(OffsetDateTime.now(), "spark:op-9").status(OperationRow.Status.CANCELLED));
    QueryOrchestrator.PollOutcome done = orchestrator.pollOnce("q-1");
    assertThat(done.done()).isTrue();
    assertThat(done.row().status()).isEqualTo(OperationRow.Status.CANCELLED);
  }
}
