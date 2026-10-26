package org.fg.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doThrow;
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
import org.fg.spi.EngineSessionStatus;
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
    "fg.command.poll.interval", "fg.query.timeout", "fg.attach.retry.cooldown",
    "fg.attach.orphan-grace"
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
    System.setProperty("fg.command.poll.interval", "10s"); // 兜底节拍设大：任何 <10s 的返回都证明是 future 唤醒
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
    when(dao.markSubmitted(anyString(), anyString()))
        .thenAnswer(
            inv -> {
              OperationRow row = rowState.get();
              if (row != null) {
                rowState.set(row.submittedAt(OffsetDateTime.now()));
              }
              return true;
            });
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
        // D39：submit 已确认（存量用例默认直通 attach 路径；门控用例显式 submittedAt(null)）
        .submittedAt(OffsetDateTime.now())
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

  /** 7：cancelSignal 完成 → InterruptedException（anyOf 立即醒）。 */
  @Test
  void cancelledPollThrows() {
    rowState.set(runningRow(OffsetDateTime.now(), "spark:op-7"));
    attachFuture = new CompletableFuture<>();
    CompletableFuture<Void> cancelSignal = new CompletableFuture<>();
    cancelSignal.complete(null); // 循环头快路径:已取消立即抛
    assertThatThrownBy(() -> orchestrator.poll("q-1", cancelSignal))
        .isInstanceOf(InterruptedException.class);
  }

  /** 7b：等待中途 cancelSignal 完成 → 毫秒级醒（非等 deadline）。 */
  @Test
  void cancelSignalWakesWaitingPoll() throws Exception {
    rowState.set(runningRow(OffsetDateTime.now(), "spark:op-7b"));
    attachFuture = new CompletableFuture<>(); // 永不完成
    CompletableFuture<Void> cancelSignal = new CompletableFuture<>();
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    Thread poller =
        new Thread(
            () -> {
              try {
                orchestrator.poll("q-1", cancelSignal);
              } catch (Throwable e) {
                thrown.set(e);
              }
            });
    poller.start();
    awaitAttachEstablished();
    long t0 = System.nanoTime();
    cancelSignal.complete(null);
    poller.join(2000);
    assertThat(thrown.get()).isInstanceOf(InterruptedException.class);
    long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
    assertThat(ms).as("cancelSignal 应毫秒级唤醒").isLessThan(1000);
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
    // D39 后语义收窄：marker 已落的 not-found = 引擎侧 operation 消失，非 never submitted
    assertThat(out2.row().error()).contains("operation not found at engine");
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

  /**
   * 10（D37）：COMMAND 跨实例（无本地命令 future、不可 attach）——唯一周期兜底分支；
   * 行迁终态后 poll 在一个 fg.command.poll.interval 内感知。
   */
  @Test
  void commandCrossInstancePollWakesWithinInterval() throws Exception {
    System.setProperty("fg.command.poll.interval", "300ms");
    config = GatewayConfig.create();
    orchestrator = new QueryOrchestrator(dao, sessions, engine, objects, config, executor);

    rowState.set(runningRow(OffsetDateTime.now(), "spark:cmd-10").kind(OperationRow.Kind.COMMAND));
    when(dao.casCompleteCommand(anyString(), any(), any()))
        .thenAnswer(inv -> migrate(OperationRow.Status.COMPLETED));

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
    Thread.sleep(150); // poll 已进入 300ms 兜底睡眠
    rowState.set(rowState.get().status(OperationRow.Status.COMPLETED)); // "触发实例"CAS 落行
    poller.join(3000);

    assertThat(result.get()).isNotNull();
    assertThat(result.get().done()).isTrue();
    verify(engine, never()).attach(any(), any()); // COMMAND 永不 attach
  }

  /** 11（D37）：cancel 引擎不可达 → UNSPECIFIED 且不改行（宁失败勿谎报）。 */
  @Test
  void cancelWithEngineUnreachableFailsWithoutTouchingRow() throws Exception {
    rowState.set(runningRow(OffsetDateTime.now(), "spark:op-11"));
    doThrow(new RuntimeException("conn refused"))
        .when(engine)
        .interrupt(any(), any());

    QueryOrchestrator.CancelOutcome outcome = orchestrator.cancel("q-1");
    assertThat(outcome).isEqualTo(QueryOrchestrator.CancelOutcome.UNSPECIFIED);
    verify(dao, never()).casCancel(anyString());
    assertThat(rowState.get().status()).isEqualTo(OperationRow.Status.RUNNING); // 行不动
  }

  /** 12（D37）：cancel 引擎可达 → interrupt 后 CAS 迁行。 */
  @Test
  void cancelWithEngineReachableMigratesRow() throws Exception {
    rowState.set(runningRow(OffsetDateTime.now(), "spark:op-12"));
    QueryOrchestrator.CancelOutcome outcome = orchestrator.cancel("q-1");
    assertThat(outcome).isEqualTo(QueryOrchestrator.CancelOutcome.CANCELLED);
    verify(engine).interrupt(any(), any());
    assertThat(rowState.get().status()).isEqualTo(OperationRow.Status.CANCELLED);
  }

  /** 13（D38）：INLINE 行 poll 不 attach（空消费 attach 会吞 arrow_batch，正确性危害）。 */
  @Test
  void inlineRowNeverAttachesViaPoll() throws Exception {
    System.setProperty("fg.query.timeout", "200ms");
    config = GatewayConfig.create();
    orchestrator = new QueryOrchestrator(dao, sessions, engine, objects, config, executor);
    rowState.set(runningRow(OffsetDateTime.now(), "spark:op-13").mode(OperationRow.Mode.INLINE));
    attachFuture = new CompletableFuture<>();

    QueryOrchestrator.PollOutcome out = orchestrator.poll("q-1");
    assertThat(out.done()).isFalse();
    verify(engine, never()).attach(any(), any()); // 信号 attach 排除
    verify(engine, never()).attachInline(any(), any(), any(), any()); // 消费 attach 只在 DoGet
  }

  /** 14（D38）：INLINE 行 cancel 成功后补 releaseExecution（driver 内存义务）。 */
  @Test
  void inlineCancelReleasesEngineCache() throws Exception {
    rowState.set(runningRow(OffsetDateTime.now(), "spark:op-14").mode(OperationRow.Mode.INLINE));
    QueryOrchestrator.CancelOutcome outcome = orchestrator.cancel("q-1");
    assertThat(outcome).isEqualTo(QueryOrchestrator.CancelOutcome.CANCELLED);
    verify(engine).releaseExecution(any());
  }

  /** 15（D38）：INLINE 终态 terminalOutcome 不 readManifest。 */
  @Test
  void inlineTerminalOutcomeSkipsManifest() throws Exception {
    rowState.set(
        runningRow(OffsetDateTime.now(), "spark:op-15")
            .mode(OperationRow.Mode.INLINE).status(OperationRow.Status.COMPLETED));
    QueryOrchestrator.PollOutcome out = orchestrator.pollOnce("q-1");
    assertThat(out.done()).isTrue();
    verify(objects, never()).readManifest(anyString());
  }

  /** 真实 register→triggerExecution 链路的公共 mock（D39 门控用例）。 */
  private AtomicReference<SqlEngine.SubmitListener> mockSubmitChain(
      CompletableFuture<EngineExecutionHandle> submitFuture) throws Exception {
    when(engine.type()).thenReturn("spark"); // bornOrGet 第三参（mock 默认 null 不匹配 anyString）
    when(engine.sessionStatus(any())).thenReturn(EngineSessionStatus.dead()); // 未锚定放行
    when(engine.analyzeSchema(any(), anyString(), any())).thenReturn(null);
    when(dao.insertOrGet(any()))
        .thenAnswer(
            inv -> {
              OperationRow r = (OperationRow) inv.getArgument(0);
              // 模拟 DB 默认列：真实 insertOrGet 后经 dao.get 重读时 created_at 已由 DB 落值
              rowState.set(
                  r.status(OperationRow.Status.RUNNING).createdAt(OffsetDateTime.now()));
              return r;
            });
    AtomicReference<SqlEngine.SubmitListener> listener = new AtomicReference<>();
    when(engine.submit(any(), anyString(), any(), any(), any()))
        .thenAnswer(
            inv -> {
              listener.set(inv.getArgument(4));
              return submitFuture;
            });
    return listener;
  }

  /**
   * 16（D39）：submit 确认门控——register 落行后、引擎首响应（operationId 回显）前，poll 不
   * 发起 attach；首响应（listener 落 marker + future 镜像完成）毫秒级唤醒后才 attach。全链
   * 路走真实 register→triggerExecution（interval=10s 下任何快速进展都证明事件驱动而非轮询）。
   */
  @Test
  void pollDuringUnsubmittedWindowWaitsForSubmitThenAttaches() throws Exception {
    // 行级 deadline 收短到 3s：poll 在 attach future 摘除竞态（D36 已注记"醒在 deadline"）下
    // 也能有界回收；interval=10s 仍证明 submit 确认的推进是事件驱动而非周期轮询
    System.setProperty("fg.query.timeout", "3s");
    config = GatewayConfig.create();
    orchestrator = new QueryOrchestrator(dao, sessions, engine, objects, config, executor);

    CompletableFuture<EngineExecutionHandle> submitFuture = new CompletableFuture<>();
    AtomicReference<SqlEngine.SubmitListener> listener = mockSubmitChain(submitFuture);
    attachFuture = new CompletableFuture<>(); // 不预完成：等门放行后由用例补

    QueryOrchestrator.Registration reg =
        orchestrator.register("s-1", "alice", "SELECT 1", OperationRow.Mode.RELAY, false, null);
    assertThat(reg.newlyTriggered()).isTrue();
    assertThat(rowState.get().submittedAt()).isNull(); // 行已出生，submit 未确认
    long t0 = System.nanoTime();
    long waitUntil = System.currentTimeMillis() + 3000;
    while (listener.get() == null && System.currentTimeMillis() < waitUntil) {
      Thread.sleep(10); // 等 triggerExecution 任务跑到 engine.submit
    }
    assertThat(listener.get()).isNotNull();

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
    Thread.sleep(200); // poll 已进入门控等待
    assertThat(attachCalls.get()).as("submit 未确认期间不得 attach").isZero();

    // 模拟引擎首响应：listener 落 marker（先）→ future 完成（后，镜像唤醒本地等待者）
    EngineExecutionHandle handle =
        EngineExecutionHandle.parse(rowState.get().connectOperationId());
    listener.get().onHandle(handle);
    submitFuture.complete(handle);

    long attachUntil = System.currentTimeMillis() + 3000;
    while (attachCalls.get() == 0 && System.currentTimeMillis() < attachUntil) {
      Thread.sleep(10);
    }
    assertThat(attachCalls.get()).as("submit 确认后应发起 attach").isGreaterThanOrEqualTo(1);
    attachFuture.complete(ExecutionOutcome.completed()); // 引擎执行完成
    poller.join(8000);

    assertThat(result.get()).isNotNull();
    assertThat(result.get().done()).isTrue();
    assertThat(result.get().row().status()).isEqualTo(OperationRow.Status.COMPLETED);
    long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
    assertThat(ms)
        .as("同实例 submit 确认应事件驱动推进（远小于 interval=10s；含 deadline 兜底上界 3s）")
        .isLessThan(5000);
  }

  /**
   * 17（D39）：跨实例（无本地 submit 信号）+ submit 始终未确认——窥行节拍（300ms）醒来到
   * grace（500ms）后标记式判死 "submit never confirmed"，全程零 attach（替代 600s sweeper）。
   */
  @Test
  void crossInstanceUnsubmittedFailsDeterministicallyBeyondGrace() throws Exception {
    System.setProperty("fg.command.poll.interval", "300ms");
    config = GatewayConfig.create();
    orchestrator = new QueryOrchestrator(dao, sessions, engine, objects, config, executor);

    rowState.set(runningRow(OffsetDateTime.now(), "spark:op-17").submittedAt(null));
    attachFuture = new CompletableFuture<>();

    long t0 = System.nanoTime();
    QueryOrchestrator.PollOutcome out = orchestrator.poll("q-1");
    long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

    assertThat(out.done()).isTrue();
    assertThat(out.row().status()).isEqualTo(OperationRow.Status.FAILED);
    assertThat(out.row().error()).contains("submit never confirmed");
    assertThat(ms).as("应在 grace(500ms) 附近判死").isBetween(400L, 3000L);
    verify(engine, never()).attach(any(), any());
  }

  /**
   * 18（D39）：submit 异常完成（marker 恒 NULL）——行先被 casFail("submit failed: ...")，
   * poll 醒来读行见终态直达真实错误，不误判 never-submitted（终态判定先于门）。
   */
  @Test
  void submitErrorWithNullMarkerSurfacesSubmitFailed() throws Exception {
    CompletableFuture<EngineExecutionHandle> submitFuture = new CompletableFuture<>();
    mockSubmitChain(submitFuture);
    attachFuture = new CompletableFuture<>();

    orchestrator.register("s-1", "alice", "SELECT 1", OperationRow.Mode.RELAY, false, null);

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
    Thread.sleep(100);
    submitFuture.completeExceptionally(new RuntimeException("engine down"));
    poller.join(5000);

    assertThat(result.get()).isNotNull();
    assertThat(result.get().done()).isTrue();
    assertThat(result.get().row().status()).isEqualTo(OperationRow.Status.FAILED);
    assertThat(result.get().row().error()).contains("submit failed");
    assertThat(result.get().row().error()).doesNotContain("never confirmed");
    assertThat(attachCalls.get()).isZero();
  }

  /**
   * 19（D39）：attachInline 门控——submit 未确认前不向引擎发起消费 attach（INLINE 快返票后
   * DoGet 几乎即刻到达，裸 attach 必撞 not-found）；marker 落（跨实例窥行）后才 attach；
   * 行终态且 marker 仍 NULL（submit 失败防御路径）→ 不 attach，行上真实错误直达。
   */
  @Test
  void attachInlineGatesOnSubmitConfirmation() throws Exception {
    System.setProperty("fg.command.poll.interval", "100ms");
    config = GatewayConfig.create();
    orchestrator = new QueryOrchestrator(dao, sessions, engine, objects, config, executor);

    AtomicInteger inlineCalls = new AtomicInteger();
    CompletableFuture<ExecutionOutcome> engineInline = new CompletableFuture<>();
    when(engine.attachInline(any(), any(), any(), any()))
        .thenAnswer(
            inv -> {
              inlineCalls.incrementAndGet();
              return engineInline;
            });
    SqlEngine.InlineConsumer consumer = mock(SqlEngine.InlineConsumer.class);

    // a：未确认 → 门内等待，零 attach；marker 落（跨实例窥行）→ attach 放行
    rowState.set(
        runningRow(OffsetDateTime.now(), "spark:op-19a")
            .mode(OperationRow.Mode.INLINE)
            .submittedAt(null));
    CompletableFuture<ExecutionOutcome> gated =
        orchestrator.attachInline(rowState.get(), null, consumer);
    Thread.sleep(250);
    assertThat(inlineCalls.get()).as("submit 未确认不得消费 attach").isZero();
    assertThat(gated.isDone()).isFalse();

    rowState.set(rowState.get().submittedAt(OffsetDateTime.now())); // 窥行看到 marker
    long t0 = System.currentTimeMillis();
    while (inlineCalls.get() == 0 && System.currentTimeMillis() - t0 < 3000) {
      Thread.sleep(10);
    }
    assertThat(inlineCalls.get()).as("marker 落后应发起消费 attach").isGreaterThanOrEqualTo(1);
    engineInline.complete(ExecutionOutcome.completed());
    assertThat(gated.get(3, TimeUnit.SECONDS).status())
        .isEqualTo(ExecutionOutcome.OutcomeStatus.COMPLETED);

    // b：终态且无 marker（submit 失败）→ 不 attach，行上错误直达
    inlineCalls.set(0);
    rowState.set(
        runningRow(OffsetDateTime.now().minusSeconds(1), "spark:op-19b")
            .mode(OperationRow.Mode.INLINE)
            .status(OperationRow.Status.FAILED)
            .error("boom: analysis failed")
            .submittedAt(null));
    CompletableFuture<ExecutionOutcome> failed =
        orchestrator.attachInline(rowState.get(), null, consumer);
    assertThatThrownBy(() -> failed.get(3, TimeUnit.SECONDS))
        .isInstanceOf(java.util.concurrent.ExecutionException.class)
        .hasRootCauseMessage("boom: analysis failed");
    assertThat(inlineCalls.get()).isZero();
  }
}
