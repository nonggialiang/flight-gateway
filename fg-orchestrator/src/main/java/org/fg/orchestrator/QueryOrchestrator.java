package org.fg.orchestrator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.vector.types.pojo.Schema;
import org.fg.common.config.GatewayConfig;
import org.fg.common.service.Service;
import org.fg.common.sql.StatementClassifier;
import org.fg.orchestrator.store.OperationRow;
import org.fg.orchestrator.store.OperationStoreDao;
import org.fg.result.manifest.ResultManifest;
import org.fg.result.store.ObjectStoreService;
import org.fg.spi.CommandOutcome;
import org.fg.spi.EngineExecutionHandle;
import org.fg.spi.EngineSession;
import org.fg.spi.ExecutionOutcome;
import org.fg.spi.GatewaySession;
import org.fg.spi.MaterializationSpec;
import org.fg.spi.SqlEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 查询编排层（design §4.2/F2）。
 *
 * <ul>
 *   <li>首个 poll/GetFlightInfo = 注册 + 触发（lazy）：指纹查行 miss → 同步 AnalyzePlan（有界，
 *       行带 schema 出生）→ 幂等 INSERT → 异步提交（提交即 detach）；
 *   <li>后续 poll = 长等待：attach 租约仲裁，赢家 Reattach 挂执行流、输家有界查行；
 *       fg.poll.max-wait 到点返回未完成；
 *   <li>取消：Interrupt（不要求 attach）+ CAS 行迁移（"B 下杀手 A 收尸"）；
 *   <li>对账兜底：等待超时且行 RUNNING → 查 manifest 存在性，存在即自助作答并修行为
 *       COMPLETED（状态以 manifest 为准，D7）。
 * </ul>
 */
public class QueryOrchestrator implements Service {

  private static final Logger logger = LoggerFactory.getLogger(QueryOrchestrator.class);

  public static final String SESSION_USER_KEY = "user";

  private final OperationStoreDao dao;
  private final SqlEngine engine;
  private final ObjectStoreService objects;
  private final GatewayConfig config;
  private final ExecutorService controlExecutor;
  private final String instanceId;

  /** attach 者内存态：queryId → 在挂的 attach future（不跨实例、不持久）。 */
  private final Map<String, CompletableFuture<ExecutionOutcome>> activeAttaches =
      new ConcurrentHashMap<>();

  public QueryOrchestrator(
      OperationStoreDao dao,
      SqlEngine engine,
      ObjectStoreService objects,
      GatewayConfig config,
      ExecutorService controlExecutor) {
    this.dao = dao;
    this.engine = engine;
    this.objects = objects;
    this.config = config;
    this.controlExecutor = controlExecutor;
    this.instanceId =
        UUID.randomUUID().toString().substring(0, 8) + "@" + hostName();
  }

  private static String hostName() {
    try {
      return java.net.InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      return "localhost";
    }
  }

  @Override
  public void start() {}

  @Override
  public void close() {
    activeAttaches.clear();
  }

  // ------------------------------------------------------------- 注册 + 触发

  /**
   * 首 poll / GetFlightInfo：先分类（D17：分类必须先于 AnalyzePlan——SET/DDL 不可分析），
   * QUERY 走幂等注册主链路；非 SELECT（SET/SHOW/DESCRIBE/EXPLAIN/USE/DDL/DML）走
   * {@link #registerCommand}（豁免指纹幂等 D19，结果内联交付 D18）。
   *
   * <p><b>行带 schema 出生</b>：先按指纹查行，miss 才<b>同步</b> AnalyzePlan（有界 prepare
   * timeout），成功后携带 schema_bytes 幂等 INSERT——"行一出现 schema 即在"，消费方（快返
   * FlightInfo 宣告 schema、终态 endpoints 构造）无需等待/回填。AnalyzePlan 失败（语法错误
   * 等）即注册失败且<b>不落行</b>（fail-fast，无半状态泄漏；语义与 plan-only prepare 一致）。
   * 并发同指纹：双双 miss → 双双分析（重复分析可接受）→ ON CONFLICT 单赢家触发，输家读回
   * 的在册行同样带 schema。
   */
  public Registration register(String sessionRef, String user, String sql, OperationRow.Mode mode)
      throws Exception {
    StatementClassifier.Kind stmtKind = StatementClassifier.classify(sql);
    if (stmtKind != StatementClassifier.Kind.QUERY) {
      return registerCommand(sessionRef, user, sql, mode, stmtKind);
    }

    String sqlHash = sha256(sql);
    OperationRow existing = dao.getByFingerprint(sessionRef, sqlHash).orElse(null);
    if (existing != null) {
      return new Registration(existing, false);
    }

    Duration prepareTimeout = Duration.ofMillis(config.getDurationMs("fg.query.prepare.timeout"));
    EngineSession session = engine.openSession(new GatewaySession(sessionRef, user, Map.of()));
    Schema schema = engine.analyzeSchema(session, sql, prepareTimeout);

    String queryId = QueryIdHolder.newQueryId();
    String resultKeyPrefix =
        config.getString("fg.result.prefix") + "/" + engine.type() + "/" + user + "/" + queryId;
    OperationRow row =
        new OperationRow()
            .queryId(queryId)
            .sessionRef(sessionRef)
            .sqlHash(sqlHash)
            .user(user)
            .sqlText(sql)
            .resultKeyPrefix(resultKeyPrefix)
            .mode(mode)
            .schemaBytes(schema == null ? null : SchemaSerde.serialize(schema))
            .engineRef(engine.type());
    OperationRow stored = dao.insertOrGet(row);
    boolean isNew = stored.queryId().equals(queryId);

    if (isNew) {
      triggerExecution(stored);
    }
    return new Registration(stored, isNew);
  }

  /**
   * 命令注册（D17/D18/D19）：新 queryId + 普通插入（豁免指纹去重，每次执行都是新行）；
   * schema=analyzable ? inline AnalyzePlan : CommandSchemas 静态宣告（ analyzable 命令
   * AnalyzePlan 失败仍 fail-fast 不落行；AnalyzePlan 无 STRUCT 时回退静态/ok 合成）。
   * resultKeyPrefix 置空串（无物化对象）；随即 triggerCommand 持流执行。
   */
  private Registration registerCommand(
      String sessionRef, String user, String sql, OperationRow.Mode mode,
      StatementClassifier.Kind stmtKind) throws Exception {
    byte[] schemaBytes;
    Schema declared = null;
    if (StatementClassifier.analyzable(stmtKind)) {
      Duration prepareTimeout = Duration.ofMillis(config.getDurationMs("fg.query.prepare.timeout"));
      EngineSession session = engine.openSession(new GatewaySession(sessionRef, user, Map.of()));
      declared = engine.analyzeSchema(session, sql, prepareTimeout);
    }
    if (declared == null) {
      declared = CommandSchemas.staticSchema(stmtKind);
      if (declared == null) {
        declared = CommandSchemas.staticSchema(StatementClassifier.Kind.DDL); // [ok BOOLEAN] 兜底
      }
    }
    schemaBytes = SchemaSerde.serialize(declared);

    String queryId = QueryIdHolder.newQueryId();
    OperationRow row =
        new OperationRow()
            .queryId(queryId)
            .sessionRef(sessionRef)
            .sqlHash(sha256(sql)) // 仍存指纹（排查/展示用），不再参与唯一约束
            .user(user)
            .sqlText(sql)
            .resultKeyPrefix("") // 命令无物化对象
            .mode(mode)
            .kind(OperationRow.Kind.COMMAND)
            .schemaBytes(schemaBytes)
            .engineRef(engine.type());
    OperationRow stored = dao.insertCommand(row);
    triggerCommand(stored);
    return new Registration(stored, true);
  }

  /**
   * 命令执行触发（controlExecutor）：executeCommand 持流至终态（网关侧无 UNKNOWN）——
   * COMPLETED 用实际 schemaBytes + 内联结果 CAS 落行（覆盖静态宣告，D18 风险③：Spark
   * INSERT 实际 schema 可能比静态宣告宽）；FAILED/CANCELLED 走 casFail/casCancel。
   */
  private void triggerCommand(OperationRow row) {
    controlExecutor.submit(
        () -> {
          try {
            EngineSession session = openEngineSession(row);
            CompletableFuture<CommandOutcome> executed =
                engine.executeCommand(
                    session,
                    row.sqlText(),
                    handle -> {
                      try {
                        dao.setOperationId(row.queryId(), handle.encode());
                      } catch (Exception e) {
                        logger.error("Failed to persist operationId for {}", row.queryId(), e);
                      }
                    });
            executed.whenComplete(
                (outcome, err) -> {
                  try {
                    if (err != null) {
                      dao.casFail(row.queryId(), "command submit failed: " + err);
                      return;
                    }
                    switch (outcome.status()) {
                      case COMPLETED ->
                          dao.casCompleteCommand(
                              row.queryId(), outcome.schemaBytes(), outcome.resultIpcBytes());
                      case FAILED ->
                          dao.casFail(
                              row.queryId(),
                              outcome.error() == null ? "command failed" : outcome.error());
                      case CANCELLED -> dao.casCancel(row.queryId());
                      default -> dao.casFail(row.queryId(), "unknown command outcome");
                    }
                  } catch (Exception e) {
                    logger.error("Failed to finalize command {}", row.queryId(), e);
                  }
                });
          } catch (Exception e) {
            logger.error("Command trigger failed for {}", row.queryId(), e);
            try {
              dao.casFail(row.queryId(), "command trigger failed: " + e);
            } catch (Exception e2) {
              logger.error("Failed to mark {} failed", row.queryId(), e2);
            }
          }
        });
  }

  /**
   * 纯 AnalyzePlan 取 schema（GetSchema / CreatePreparedStatement 用）：不建 OperationRow、
   * 不触发执行——Flight SQL 语义上这两个 RPC 是 plan-only。非查询语句（无 STRUCT 结果）
   * 返回 null。失败/超时直接抛出，由调用方决定错误面。
   */
  public byte[] analyzeSchema(String sessionRef, String user, String sql) throws Exception {
    Duration prepareTimeout = Duration.ofMillis(config.getDurationMs("fg.query.prepare.timeout"));
    // engine session 缓存策略与 triggerExecution 同款，后续按需增强
    EngineSession session = engine.openSession(new GatewaySession(sessionRef, user, Map.of()));
    Schema schema = engine.analyzeSchema(session, sql, prepareTimeout);
    return schema == null ? null : SchemaSerde.serialize(schema);
  }

  /**
   * 异步提交（提交即 detach，首响应回调落 operationId）。schema 已随行出生（register 同步
   * AnalyzePlan），此处只做排序检测 + 物化规划 + 提交。
   */
  private void triggerExecution(OperationRow row) {
    controlExecutor.submit(() -> {
      try {
        EngineSession session = openEngineSession(row);
        try {
          try {
            boolean ordered = engine.isOrderSensitive(session, row.sqlText());
            dao.setOrdered(row.queryId(), ordered);
            row.ordered(ordered);
          } catch (Exception e) {
            logger.warn("Order detection failed for {}: {}", row.queryId(), e.toString());
          }

          // H4：partitions hint 裁决——顺序敏感：HTTPS 强制单对象；RELAY 禁 repartition(K) 用自然分区
          Integer configured = null;
          if (config.hasPath("fg.result.partitions")) {
            configured = config.getInt("fg.result.partitions");
          }
          Integer partitionsHint = configured;
          if (row.ordered()) {
            partitionsHint = row.mode() == OperationRow.Mode.HTTPS ? 1 : null;
          }

          MaterializationSpec spec =
              new MaterializationSpec(
                  // s3a：Hadoop FileSystem 的实际 scheme（s3:// 无 FS 实现）
                  "s3a://" + objects.bucket() + "/" + row.resultKeyPrefix() + "/",
                  true,
                  config.getInt("fg.result.batch.max.records"),
                  partitionsHint);

          CompletableFuture<EngineExecutionHandle> submitted =
              engine.submit(
                  session,
                  row.sqlText(),
                  spec,
                  handle -> {
                    try {
                      dao.setOperationId(row.queryId(), handle.encode());
                    } catch (Exception e) {
                      logger.error("Failed to persist operationId for {}", row.queryId(), e);
                    }
                  });
          submitted.whenComplete(
              (handle, err) -> {
                try {
                  if (err != null) {
                    dao.casFail(row.queryId(), "submit failed: " + err);
                  }
                } catch (Exception e) {
                  logger.error("Failed to mark {} failed", row.queryId(), e);
                }
              });
        } finally {
          // 触发实例提交后即无状态；engine session 缓存策略后续按需增强
        }
      } catch (Exception e) {
        logger.error("Trigger failed for {}", row.queryId(), e);
        try {
          dao.casFail(row.queryId(), "trigger failed: " + e);
        } catch (Exception e2) {
          logger.error("Failed to mark {} failed", row.queryId(), e2);
        }
      }
    });
  }

  private EngineSession openEngineSession(OperationRow row) throws Exception {
    return engine.openSession(new GatewaySession(row.sessionRef(), row.user(), Map.of()));
  }

  // ------------------------------------------------------------- poll 长等待

  /**
   * 后续 poll：长等待至终态或 maxWait 到点。返回的 PollOutcome.done=false 表示仍未完成
   * （客户端续 poll）。
   */
  public PollOutcome poll(String queryId, Duration maxWait) throws Exception {
    return poll(queryId, maxWait, () -> false);
  }

  /**
   * 带取消感知的 poll：cancelled 为 true（如 DoGet 客户端断流）时立即以 InterruptedException
   * 退出，释放有界 relay 池线程；每轮 DB 轮询间隙检查一次（粒度 fg.poll.db.interval）。
   */
  public PollOutcome poll(
      String queryId, Duration maxWait, java.util.function.BooleanSupplier cancelled)
      throws Exception {
    long deadline = System.nanoTime() + maxWait.toNanos();
    while (true) {
      if (cancelled.getAsBoolean()) {
        throw new InterruptedException("Poll caller cancelled: " + queryId);
      }
      Optional<OperationRow> rowOpt = dao.get(queryId);
      if (rowOpt.isEmpty()) {
        throw new IllegalArgumentException("Unknown query: " + queryId);
      }
      OperationRow row = rowOpt.get();
      if (row.status().terminal()) {
        return terminalOutcome(row);
      }
      // RUNNING：尝试成为 attach 者（赢家挂流，输家查行等待）
      maybeAttach(row);
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        // 对账兜底：等待到点仍 RUNNING → 查 manifest（触发实例死亡的修复路径）
        if (reconcile(queryId)) {
          return terminalOutcome(dao.get(queryId).orElseThrow());
        }
        return new PollOutcome(false, null, null);
      }
      // 单位统一到毫秒再取小（曾纳秒/毫秒错配：min 几乎恒取 interval → 固定睡一个
      // interval、max-wait 被 overshoot 一整个 interval，醒来先查终态常直接终态返回）
      long sleep =
          Math.min(
              TimeUnit.NANOSECONDS.toMillis(remaining),
              config.getDurationMs("fg.poll.db.interval"));
      TimeUnit.MILLISECONDS.sleep(sleep);
    }
  }

  private PollOutcome terminalOutcome(OperationRow row) {
    if (row.kind() == OperationRow.Kind.COMMAND) {
      // D18：命令结果内联行内（command_result），无 manifest——跳过 readManifest
      return new PollOutcome(true, row, null);
    }
    if (row.status() == OperationRow.Status.COMPLETED) {
      try {
        ResultManifest manifest = objects.readManifest(row.resultKeyPrefix());
        return new PollOutcome(true, row, manifest);
      } catch (Exception e) {
        logger.error("Manifest read failed for {} (reporting FAILED): {}", row.queryId(), e.toString());
        return new PollOutcome(true, row.status(OperationRow.Status.FAILED).error("manifest missing: " + e), null);
      }
    }
    return new PollOutcome(true, row, null);
  }

  /** attach 租约仲裁 + 挂流。赢家：流终→CAS 迁移；UNKNOWN→manifest 对账。 */
  private void maybeAttach(OperationRow row) {
    if (row.kind() == OperationRow.Kind.COMMAND) {
      return; // D18：命令由触发实例持流至终态，无 attach 语义（也无 manifest 可对账）
    }
    if (row.connectOperationId() == null) {
      return; // 触发尚未捕获 operationId（提交在途）
    }
    if (activeAttaches.containsKey(row.queryId())) {
      return; // 本实例已有在挂 attach
    }
    try {
      boolean won =
          dao.tryAcquireAttachLease(
              row.queryId(), instanceId, Duration.ofMillis(config.getDurationMs("fg.poll.max-wait") * 2));
      if (!won) {
        return;
      }
    } catch (Exception e) {
      logger.warn("Lease acquire failed for {}: {}", row.queryId(), e.toString());
      return;
    }

    CompletableFuture<ExecutionOutcome> future =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                EngineSession session = openEngineSession(row);
                EngineExecutionHandle handle =
                    EngineExecutionHandle.parse(row.connectOperationId());
                return engine.attach(session, handle).join();
              } catch (Exception e) {
                return ExecutionOutcome.unknown(e.toString());
              }
            },
            controlExecutor);
    activeAttaches.put(row.queryId(), future);
    future.whenComplete(
        (outcome, err) -> {
          activeAttaches.remove(row.queryId());
          try {
            if (err != null || outcome == null) {
              outcome = ExecutionOutcome.unknown(String.valueOf(err));
            }
            switch (outcome.status()) {
              case COMPLETED -> dao.casComplete(row.queryId());
              case FAILED -> dao.casFail(row.queryId(), outcome.error());
              case CANCELLED -> dao.casCancel(row.queryId());
              case UNKNOWN -> {
                // 状态以 manifest 为准（D7）：流断≠失败
                if (!reconcile(row.queryId())) {
                  logger.info(
                      "Attach UNKNOWN for {} (lease released, next poll re-attaches)", row.queryId());
                }
                dao.clearAttach(row.queryId(), instanceId);
              }
            }
          } catch (Exception e) {
            logger.error("Post-attach row migration failed for {}", row.queryId(), e);
          } finally {
            try {
              if (row.connectOperationId() != null) {
                engine.releaseExecution(EngineExecutionHandle.parse(row.connectOperationId()));
              }
            } catch (Exception e) {
              logger.debug("ReleaseExecution failed for {}: {}", row.queryId(), e.toString());
            }
          }
        });
  }

  /** 对账兜底：行 RUNNING 但 manifest 在 → 修行为 COMPLETED（manifest 是真相源）。 */
  public boolean reconcile(String queryId) {
    try {
      Optional<OperationRow> rowOpt = dao.get(queryId);
      if (rowOpt.isEmpty() || rowOpt.get().status().terminal()) {
        return false;
      }
      if (rowOpt.get().kind() == OperationRow.Kind.COMMAND) {
        return false; // 命令无 manifest；孤儿行由 QueryTimeoutSweeper 在 fg.query.timeout 兜底 FAILED
      }
      OperationRow row = rowOpt.get();
      if (!objects.manifestExists(row.resultKeyPrefix())) {
        return false;
      }
      boolean migrated = dao.casComplete(queryId);
      if (migrated && row.schemaBytes() == null) {
        try {
          ResultManifest manifest = objects.readManifest(row.resultKeyPrefix());
          if (manifest.schemaBase64() != null) {
            dao.setSchema(
                queryId,
                java.util.Base64.getDecoder().decode(manifest.schemaBase64()));
          }
        } catch (Exception e) {
          logger.warn("Schema backfill from manifest failed for {}: {}", queryId, e.toString());
        }
      }
      logger.info("Reconciled {} to COMPLETED via manifest", queryId);
      return true;
    } catch (Exception e) {
      logger.warn("Reconcile failed for {}: {}", queryId, e.toString());
      return false;
    }
  }

  // ------------------------------------------------------------- 取消

  /** CancelFlightInfo："B 下杀手"。引擎不可达=UNSPECIFIED（客户端可重试）。 */
  public CancelOutcome cancel(String queryId) {
    try {
      Optional<OperationRow> rowOpt = dao.get(queryId);
      if (rowOpt.isEmpty()) {
        return CancelOutcome.NOT_CANCELLABLE;
      }
      OperationRow row = rowOpt.get();
      if (row.status().terminal()) {
        return CancelOutcome.NOT_CANCELLABLE;
      }
      if (row.connectOperationId() != null) {
        try {
          EngineSession session = openEngineSession(row);
          engine.interrupt(session, EngineExecutionHandle.parse(row.connectOperationId()));
        } catch (Exception e) {
          // 引擎不可达：CAS 迁移仍尝试；返回 UNSPECIFIED 由调用方决定
          boolean migrated = dao.casCancel(queryId);
          return migrated ? CancelOutcome.CANCELLED : CancelOutcome.NOT_CANCELLABLE;
        }
      }
      boolean migrated = dao.casCancel(queryId);
      return migrated ? CancelOutcome.CANCELLED : CancelOutcome.NOT_CANCELLABLE;
    } catch (Exception e) {
      logger.error("Cancel failed for {}", queryId, e);
      return CancelOutcome.UNSPECIFIED;
    }
  }

  // ------------------------------------------------------------- helpers

  public static String sha256(String s) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return HexFormat.of().formatHex(digest.digest(s.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static final class QueryIdHolder {
    static String newQueryId() {
      return UUID.randomUUID().toString();
    }
  }

  public record Registration(OperationRow row, boolean newlyTriggered) {}

  public record PollOutcome(boolean done, OperationRow row, ResultManifest manifest) {}

  public enum CancelOutcome {
    CANCELLED,
    NOT_CANCELLABLE,
    UNSPECIFIED
  }
}
