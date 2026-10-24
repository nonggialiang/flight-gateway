package org.fg.orchestrator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import org.fg.orchestrator.store.SessionRegistryDao;
import org.fg.result.manifest.ResultManifest;
import org.fg.result.store.ObjectStoreService;
import org.fg.spi.CommandOutcome;
import org.fg.spi.EngineCatalog;
import org.fg.spi.EngineExecutionHandle;
import org.fg.spi.EngineSession;
import org.fg.spi.EngineSessionStatus;
import org.fg.spi.ExecutionOutcome;
import org.fg.spi.GatewaySession;
import org.fg.spi.MaterializationSpec;
import org.fg.spi.SqlEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 查询编排层（design §4.2/F2；D36 去 attach 租约）。
 *
 * <ul>
 *   <li>首个 poll/GetFlightInfo = 注册 + 触发（lazy）：指纹查行 miss → 同步 AnalyzePlan（有界，
 *       行带 schema 出生）→ 幂等 INSERT（<b>行带预生成 operationId 出生</b>——D36：Spark
 *       Connect 客户端可指定 operation_id，submit 随请求下发）→ 异步提交（提交即 detach）；
 *   <li>后续 poll = 长等待：等本实例 attach future（D36 无跨实例租约——各实例各自 attach 同一
 *       operationId、实例内去重；同实例所有 poll 共享 get 同一 future，毫秒级唤醒）。等待预算
 *       为行级 deadline：row.created_at + fg.query.timeout（与 sweeper 同值同时到点，客户端零
 *       续期）；
 *   <li>取消：Interrupt（不要求 attach）+ CAS 行迁移（"B 下杀手 A 收尸"）；
 *   <li>对账兜底：等待到点且行 RUNNING → 查 manifest 存在性，存在即自助作答并修行为
 *       COMPLETED（状态以 manifest 为准，D7）。
 * </ul>
 */
public class QueryOrchestrator implements Service {

  private static final Logger logger = LoggerFactory.getLogger(QueryOrchestrator.class);

  public static final String SESSION_USER_KEY = "user";

  private final OperationStoreDao dao;
  private final SessionRegistryDao sessions;
  private final SqlEngine engine;
  private final ObjectStoreService objects;
  private final GatewayConfig config;
  private final ExecutorService controlExecutor;

  /** attach 者内存态：queryId → 在挂的 attach future（不跨实例、不持久；D36 各实例各自建）。 */
  private final Map<String, CompletableFuture<ExecutionOutcome>> activeAttaches =
      new ConcurrentHashMap<>();

  /**
   * D36：UNKNOWN 后的重试冷却表（queryId → epoch millis 之前不可重建 attach）。防引擎死/
   * 抢跑窗口的 tight loop；不进 activeAttaches（占位 future 会与 poll 的 get 语义混淆）。
   */
  private final Map<String, Long> attachRetryNotBefore = new ConcurrentHashMap<>();

  public QueryOrchestrator(
      OperationStoreDao dao,
      SessionRegistryDao sessions,
      SqlEngine engine,
      ObjectStoreService objects,
      GatewayConfig config,
      ExecutorService controlExecutor) {
    this.dao = dao;
    this.sessions = sessions;
    this.engine = engine;
    this.objects = objects;
    this.config = config;
    this.controlExecutor = controlExecutor;
  }

  @Override
  public void start() {}

  @Override
  public void close() {
    // D36：中断 join 线程加速关停（cancel(true) 令 supplyAsync 内 join 抛 CancellationException）
    activeAttaches.values().forEach(f -> f.cancel(true));
    activeAttaches.clear();
    attachRetryNotBefore.clear();
  }

  // ------------------------------------------------------------- 会话生命周期（D20）

  /** 会话已关闭（终态 sticky）：客户端须换新会话身份（重连取新 cookie / 轮换 x-fg-session-id）。 */
  public static final class SessionClosedException extends RuntimeException {
    public final String reason; // client | engine_lost

    public SessionClosedException(String reason) {
      super("Session closed (" + reason + "); re-establish session"
          + ("engine_lost".equals(reason) ? " — engine-side session is gone (evicted/restarted)" : ""));
      this.reason = reason;
    }
  }

  /**
   * 会话解析（D20 生命周期绑定，所有客户端入口 RPC 必经）：born-or-get 登记 → CLOSED 即拒
   * （sticky）→ 化身校验（engine_started_at 已锚定者比对引擎侧 status：!alive 或 startedAt
   * 不符 = 引擎侧已死/转世 → markClosed(engine_lost) 即拒；未锚定者不校验——首个真实
   * Connect RPC 才会让引擎侧会话诞生，且一旦 alive 即锚定）。touch 活跃度。
   *
   * <p>校验成本 = 每次 RPC 一次 engine admin 查询（kit 内实现，无 Connect 副作用）；poll
   * 续轮不走此处（操作已在途，会话状态与其无关）。
   */
  private GatewaySession resolveSession(String sessionRef, String user) throws Exception {
    SessionRegistryDao.SessionRow row = sessions.bornOrGet(sessionRef, user, engine.type()).row();
    if (row.status() == SessionRegistryDao.Status.CLOSED) {
      throw new SessionClosedException(row.closedReason() == null ? "client" : row.closedReason());
    }
    GatewaySession ctx = new GatewaySession(sessionRef, user);
    if (row.engineStartedAt() != null) {
      EngineSessionStatus st = engine.sessionStatus(engine.openSession(ctx));
      if (!st.alive() || st.engineStartedAt() != row.engineStartedAt()) {
        sessions.markClosed(sessionRef, "engine_lost");
        logger.warn("Session {} engine-side gone (alive={} latched={} actual={})",
            sessionRef, st.alive(), row.engineStartedAt(), st.engineStartedAt());
        throw new SessionClosedException("engine_lost");
      }
    } else {
      // 未锚定：alive 即锚定当前化身；!alive 视为尚未与引擎接触（预诞生），放行
      EngineSessionStatus st = engine.sessionStatus(engine.openSession(ctx));
      if (st.alive()) {
        sessions.markEngineStarted(sessionRef, st.engineStartedAt());
      }
    }
    sessions.touch(sessionRef);
    return ctx;
  }

  /** 操作续行路径（poll/attach/触发异步体）取会话：只取化身 id，不做生命周期校验。 */
  private GatewaySession sessionFor(OperationRow row) throws Exception {
    SessionRegistryDao.SessionRow s =
        sessions.bornOrGet(row.sessionRef(), row.user(), row.engineRef()).row();
    return new GatewaySession(row.sessionRef(), row.user());
  }

  /**
   * 化身锚定补针：resolveSession 时引擎会话可能尚未诞生（首个真实 RPC 在其后）——锚定
   * 推迟到首次引擎接触完成后（QUERY：同步 AnalyzePlan 后；COMMAND：执行终态后）。
   * 不锚定的窗口内引擎重启会被误判"预诞生"放行（engine_lost 漏检），故必补。
   */
  private void latchEngineStarted(String sessionRef, String user) {
    try {
      SessionRegistryDao.SessionRow row = sessions.get(sessionRef).orElse(null);
      if (row == null || row.status() != SessionRegistryDao.Status.ACTIVE
          || row.engineStartedAt() != null) {
        return;
      }
      EngineSessionStatus st =
          engine.sessionStatus(
              engine.openSession(new GatewaySession(sessionRef, user)));
      if (st.alive()) {
        sessions.markEngineStarted(sessionRef, st.engineStartedAt());
      }
    } catch (Exception e) {
      logger.debug("Engine-started latch skipped for {}: {}", sessionRef, e.toString());
    }
  }

  /**
   * SetSessionOptions（D20 不落盘）：即时代理到引擎会话 conf；option_keys 登记仅供
   * {@link #getSessionOptions} 回读。空值清除（unset）正合 Flight SQL SessionOptionValue 语义。
   */
  public void applySessionOptions(
      String sessionRef, String user, Map<String, String> toSet, Set<String> toUnset)
      throws Exception {
    GatewaySession ctx = resolveSession(sessionRef, user);
    engine.setSessionConf(engine.openSession(ctx), toSet, toUnset);
    sessions.mergeOptionKeys(sessionRef, toSet.keySet(), toUnset);
  }

  /** GetSessionOptions：已登记键，值实时取引擎会话（会话死即无值——键随 CLOSED 行留存但读不出）。 */
  public Map<String, String> getSessionOptions(String sessionRef, String user) throws Exception {
    GatewaySession ctx = resolveSession(sessionRef, user);
    List<String> keys =
        sessions.get(sessionRef).map(SessionRegistryDao.SessionRow::optionKeys).orElse(List.of());
    if (keys.isEmpty()) {
      return Map.of();
    }
    return engine.getSessionConf(engine.openSession(ctx), keys);
  }

  /** CloseSession（客户端显式关闭）：引擎侧释放（在途执行中断 + 会话逐出）→ 行 CLOSED(client)。 */
  public void closeSession(String sessionRef, String user) throws Exception {
    SessionRegistryDao.SessionRow row = sessions.get(sessionRef).orElse(null);
    if (row == null || row.status() == SessionRegistryDao.Status.CLOSED) {
      return; // 未知会话无物可关；幂等
    }
    engine.closeSession(
        engine.openSession(new GatewaySession(sessionRef, user)));
    sessions.markClosed(sessionRef, "client");
    logger.info("Session {} closed by client (engine session released)", sessionRef);
  }

  // ------------------------------------------------------------- 元数据目录（D21，M2）

  /** 目录表概要（frontend 不直碰 SPI——orchestrator 自带小 record）。 */
  public record TableSummary(
      String catalog, String database, String name, String type, boolean temporary) {}

  /**
   * 元数据目录出口（三方法，均 resolveSession 必经 + engine.catalog 收敛调用；同步内联
   * 小结果，不建 fg_operation 行、无 poll 语义——与 GetSchema plan-only 同类，D21）。
   * pattern 过滤归 frontend（SqlPatternMatcher），此处返回引擎原始清单。
   */
  public List<String> catalogCatalogs(String sessionRef, String user) throws Exception {
    GatewaySession ctx = resolveSession(sessionRef, user);
    return engine.catalog(engine.openSession(ctx)).listCatalogs();
  }

  public List<String> catalogDatabases(String sessionRef, String user, String catalog)
      throws Exception {
    GatewaySession ctx = resolveSession(sessionRef, user);
    return engine.catalog(engine.openSession(ctx)).listDatabases(catalog).stream()
        .map(EngineCatalog.EngineDatabase::name)
        .collect(java.util.stream.Collectors.toList());
  }

  public List<TableSummary> catalogTables(
      String sessionRef, String user, String catalog, String database) throws Exception {
    GatewaySession ctx = resolveSession(sessionRef, user);
    return engine.catalog(engine.openSession(ctx)).listTables(catalog, database).stream()
        .map(t -> new TableSummary(t.catalog(), t.database(), t.name(), t.type(), t.isTemporary()))
        .collect(java.util.stream.Collectors.toList());
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
  public Registration register(
      String sessionRef, String user, String sql, OperationRow.Mode mode, boolean scrollable)
      throws Exception {
    return register(sessionRef, user, sql, mode, scrollable, null);
  }

  /**
   * D28 显式续传协议（x-fg-query-id 头，fork 驱动专用）：
   *
   * <pre>
   * ① requested 存在且 (session, queryId) 命中 + sql_hash 匹配 → 续传该行（终态复用 /
   *    RUNNING poll 收敛；引擎不跑）
   * ①' 命中但 sql_hash 不匹配 → INVALID_ARGUMENT（显式报错：旧票配新 SQL 是客户端 bug，
   *    静默落穿会掩盖）
   * ② requested 存在但未命中 → 直接新建执行（queryId = 客户端传入值原样存储；指纹去重
   *    完全不参与——这是 fork 驱动主动重跑 SQL 的手段）
   * ③ requested 缺失（无头客户端 pyarrow/ADBC）→ 现行指纹去重原样（含终态复用）
   * </pre>
   *
   * <p>queryId 校验：非空、≤128 字符（存 TEXT 的注入面边界）。
   */
  public Registration register(
      String sessionRef,
      String user,
      String sql,
      OperationRow.Mode mode,
      boolean scrollable,
      String requestedQueryId)
      throws Exception {
    GatewaySession ctx = resolveSession(sessionRef, user); // D20：入口必经（CLOSED sticky 即拒）
    StatementClassifier.Kind stmtKind = StatementClassifier.classify(sql);
    if (stmtKind != StatementClassifier.Kind.QUERY) {
      // scroll/续传只对 QUERY 有意义（COMMAND 终态恒单 COMMAND endpoint 内联交付）
      return registerCommand(ctx, sql, mode, stmtKind);
    }

    String sqlHash = sha256(sql);
    final String explicitQueryId;
    if (requestedQueryId != null && !requestedQueryId.isBlank()) {
      if (requestedQueryId.length() > 128) {
        throw new IllegalArgumentException("x-fg-query-id too long (max 128): fork driver bug?");
      }
      OperationRow byId = dao.getBySessionAndQueryId(sessionRef, requestedQueryId).orElse(null);
      if (byId != null) { // ① / ①'
        if (!byId.sqlHash().equals(sqlHash)) {
          throw new QueryIdMismatchException(
              "x-fg-query-id " + requestedQueryId + " belongs to a different SQL");
        }
        return new Registration(byId, false);
      }
      explicitQueryId = requestedQueryId; // ②：新执行，客户端铸造的 id 即行身份
    } else {
      explicitQueryId = null; // ③：无头路径
    }

    if (explicitQueryId == null) { // ③ 指纹去重（含终态复用）
      OperationRow existing = dao.getByFingerprint(sessionRef, sqlHash).orElse(null);
      if (existing != null) {
        return new Registration(existing, false);
      }
    }

    Duration prepareTimeout = Duration.ofMillis(config.getDurationMs("fg.query.prepare.timeout"));
    Schema schema = engine.analyzeSchema(engine.openSession(ctx), sql, prepareTimeout);
    latchEngineStarted(ctx.sessionId(), ctx.user()); // 引擎会话已随 AnalyzePlan 诞生

    String queryId = explicitQueryId != null ? explicitQueryId : QueryIdHolder.newQueryId();
    String resultKeyPrefix =
        config.getString("fg.result.prefix") + "/" + engine.type() + "/" + user + "/" + queryId;
    // D36：预生成 operationId 随行出生——Spark Connect 客户端可指定 operation_id（引擎按
    // UUID 规范化作执行身份），行 INSERT 起即可 attach/interrupt/release，消除"提交在途
    // opId 未知"窗口（UUID 标准形态保证引擎规范化 round-trip 无歧义）。
    EngineExecutionHandle clientHandle =
        new EngineExecutionHandle(engine.type(), UUID.randomUUID().toString());
    OperationRow row =
        new OperationRow()
            .queryId(queryId)
            .sessionRef(sessionRef)
            .sqlHash(sqlHash)
            .user(user)
            .sqlText(sql)
            .resultKeyPrefix(resultKeyPrefix)
            .mode(mode)
            .scrollable(scrollable) // D27：注册时落行（同 mode 的"注册时协商"语义）
            .schemaBytes(schema == null ? null : SchemaSerde.serialize(schema))
            .connectOperationId(clientHandle.encode())
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
      GatewaySession ctx, String sql, OperationRow.Mode mode,
      StatementClassifier.Kind stmtKind) throws Exception {
    byte[] schemaBytes;
    Schema declared = null;
    if (StatementClassifier.analyzable(stmtKind)) {
      Duration prepareTimeout = Duration.ofMillis(config.getDurationMs("fg.query.prepare.timeout"));
      declared = engine.analyzeSchema(engine.openSession(ctx), sql, prepareTimeout);
    }
    if (declared == null) {
      declared = CommandSchemas.staticSchema(stmtKind);
      if (declared == null) {
        declared = CommandSchemas.staticSchema(StatementClassifier.Kind.DDL); // [ok BOOLEAN] 兜底
      }
    }
    schemaBytes = SchemaSerde.serialize(declared);

    String queryId = QueryIdHolder.newQueryId(); // COMMAND 不吃客户端 queryId（内联交付无续传语义）
    OperationRow row =
        new OperationRow()
            .queryId(queryId)
            .sessionRef(ctx.sessionId())
            .sqlHash(sha256(sql)) // 仍存指纹（排查/展示用），不再参与唯一约束
            .user(ctx.user())
            .sqlText(sql)
            .resultKeyPrefix("") // 命令无物化对象
            .mode(mode)
            .kind(OperationRow.Kind.COMMAND)
            .schemaBytes(schemaBytes)
            .engineRef(engine.type());
    OperationRow stored = dao.insertCommandOrGet(row);
    boolean isNew = stored.queryId().equals(queryId);
    // 在途幂等（D19 修正）：同 (session, sql) RUNNING 行已存在（PollFlightInfo 逐次
    // register 的后续 poll）→ 复用，不重触发；终态后索引释放，新执行照常新行。
    if (isNew) {
      triggerCommand(stored);
    }
    return new Registration(stored, isNew);
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
                    latchEngineStarted(row.sessionRef(), row.user()); // 引擎会话已随执行诞生
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
    GatewaySession ctx = resolveSession(sessionRef, user); // D20：入口必经
    Schema schema = engine.analyzeSchema(engine.openSession(ctx), sql, prepareTimeout);
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
                  config.getString("fg.result.compression"),
                  config.getInt("fg.result.batch.max.records"),
                  partitionsHint);

          // D36：预生成句柄随请求下发（行 INSERT 已带）；回显校验在 engine 侧（不一致仅 warn）。
          EngineExecutionHandle clientHandle =
              EngineExecutionHandle.parse(row.connectOperationId());
          CompletableFuture<EngineExecutionHandle> submitted =
              engine.submit(
                  session,
                  row.sqlText(),
                  spec,
                  clientHandle,
                  handle -> {
                    // 行已带同值 opId；此写为幂等兜底（滚动升级期旧版本行/异常路径）。
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
    // 续行路径（触发异步体/attach）：经登记表取化身 id，不做生命周期校验（操作已在途）
    return engine.openSession(sessionFor(row));
  }

  // ------------------------------------------------------------- poll 长等待

  /** 后续 poll（无取消感知）：见 {@link #poll(String, java.util.function.BooleanSupplier)}。 */
  public PollOutcome poll(String queryId) throws Exception {
    return poll(queryId, () -> false);
  }

  /**
   * 零等待单查（PART 票/scroll 页票，D15/D27 语义）：查行一次——终态即返；RUNNING 则做一次
   * manifest 对账（触发实例死亡的修复路径）后仍非终态即 not-done（调用方转 UNAVAILABLE
   * 快速暴露，不占 relay 池长挂）。不建 attach、不等待。
   */
  public PollOutcome pollOnce(String queryId) throws Exception {
    Optional<OperationRow> rowOpt = dao.get(queryId);
    if (rowOpt.isEmpty()) {
      throw new IllegalArgumentException("Unknown query: " + queryId);
    }
    OperationRow row = rowOpt.get();
    if (row.status().terminal()) {
      return terminalOutcome(row);
    }
    if (reconcile(queryId)) {
      return terminalOutcome(dao.get(queryId).orElseThrow());
    }
    return new PollOutcome(false, null, null);
  }

  /**
   * D36：poll 长等待——预算为<b>行级</b> deadline（row.created_at + fg.query.timeout，与
   * sweeper 同值同时到点；任何实例算出同一 deadline，客户端零续期），返回条件：终态 / 行级
   * deadline 到点（sweeper 届时已/正迁 FAILED，返终态或 not-done 由客户端重 poll 即见）。
   *
   * <p>等待机制：等本实例 attach future（{@link CompletableFuture#get(long, TimeUnit)} 毫秒级
   * 唤醒；同实例所有 poll 共享同一 future）。TimeoutException 不是失败，是<b>心跳</b>——回圈
   * 重读行，tick（fg.poll.db.interval）作为安全网上限同时承担：① cancelled（DoGet 断流）
   * 检查粒度；② 引擎 wedge（attach 流永不完成）时经重读行感知 sweeper 迁移；③ 行被旁路
   * 迁移（cancel RPC/他实例 reconcile）的兜底可见性。
   */
  public PollOutcome poll(String queryId, java.util.function.BooleanSupplier cancelled)
      throws Exception {
    long deadlineEpochMs = Long.MIN_VALUE; // 首轮读到行后初始化（行级 deadline）
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
      if (deadlineEpochMs == Long.MIN_VALUE) {
        deadlineEpochMs =
            row.createdAt().toInstant().toEpochMilli()
                + config.getDurationMs("fg.query.timeout");
      }
      // RUNNING：确保本实例有 attach 在挂（D36 各实例各自建，实例内 putIfAbsent 去重）
      maybeAttach(row);
      long remainingMs = deadlineEpochMs - System.currentTimeMillis();
      if (remainingMs <= 0) {
        // 对账兜底：行级 deadline 到点仍 RUNNING → 查 manifest（触发实例死亡的修复路径）；
        // manifest 不在则 not-done（sweeper 与本 deadline 同值，客户端重 poll 即见 FAILED）
        if (reconcile(queryId)) {
          return terminalOutcome(dao.get(queryId).orElseThrow());
        }
        return new PollOutcome(false, null, null);
      }
      long waitMs = Math.min(remainingMs, config.getDurationMs("fg.poll.db.interval"));

      CompletableFuture<ExecutionOutcome> attach = activeAttaches.get(queryId);
      if (attach == null) {
        // maybeAttach 刚拒绝建（非"没去 attach"）。新行 opId 随 INSERT 存在，只剩：①滚动升级
        // 期旧实例建的行 opId null（maybeAttach 拒建，退回 D36 前行为：tick+sweeper）；②
        // cooldown 未到（UNKNOWN 后主动不建）；③COMMAND 行（无 attach 语义）。
        // 良性竞态：future 建后瞬间完成已被 whenComplete 移除 → 本轮 sleep，下圈重读行即见结果。
        TimeUnit.MILLISECONDS.sleep(waitMs);
        continue;
      }
      try {
        attach.get(waitMs, TimeUnit.MILLISECONDS); // ★ 毫秒级唤醒；返回值丢弃——响应以重读行为准
        OperationRow after = dao.get(queryId).orElseThrow();
        if (after.status().terminal()) {
          return terminalOutcome(after);
        }
        // 行仍 RUNNING = UNKNOWN 且 reconcile 未修 → 回圈，受 cooldown 节流
      } catch (java.util.concurrent.TimeoutException te) {
        // tick 心跳：回圈重读行（兜底对账由行级 deadline 分支负责）
      } catch (java.util.concurrent.ExecutionException ee) {
        // supplyAsync 已兜异常转 UNKNOWN，理论不可达；防御性回圈
      }
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

  /**
   * D36：确保本实例有 attach 在挂（无跨实例租约——各实例各自 attach 同一 operationId，多挂
   * 由 CAS 条件更新幂等收敛；被 release 切流者经 UNKNOWN→reconcile 归位）。实例内去重：
   * putIfAbsent 防双 poll 并发建。UNKNOWN 后进 cooldown 防引擎死/抢跑窗口的 tight loop。
   */
  private void maybeAttach(OperationRow row) {
    if (row.kind() == OperationRow.Kind.COMMAND) {
      return; // D18：命令由触发实例持流至终态，无 attach 语义（也无 manifest 可对账）
    }
    if (row.connectOperationId() == null) {
      return; // 滚动升级兼容：旧实例建的行无 opId（新行随 INSERT 带）——退回 tick+sweeper
    }
    Long notBefore = attachRetryNotBefore.get(row.queryId());
    if (notBefore != null) {
      if (System.currentTimeMillis() < notBefore) {
        return;
      }
      attachRetryNotBefore.remove(row.queryId(), notBefore); // CAS remove 防并发重复清理
    }

    // computeIfAbsent：原子单建（putIfAbsent + 急切 supplyAsync 会令并发双 poll 各自启动
    // attach 任务，后来者的流成引擎侧孤儿）；善后 handler 由创建者恰好注册一次
    activeAttaches.computeIfAbsent(
        row.queryId(),
        k -> {
          CompletableFuture<ExecutionOutcome> future =
              CompletableFuture.supplyAsync(
                  () -> {
                    try {
                      EngineSession session = openEngineSession(row);
                      EngineExecutionHandle handle =
                          EngineExecutionHandle.parse(row.connectOperationId());
                      return engine.attach(session, handle).join();
                    } catch (Exception e) {
                      return ExecutionOutcome.unknown(e.toString()); // supplyAsync 永不异常完成
                    }
                  },
                  controlExecutor);
          future.whenComplete(
              (outcome, err) -> {
                try {
                  if (err != null || outcome == null) {
                    outcome = ExecutionOutcome.unknown(String.valueOf(err));
                  }
                  switch (outcome.status()) {
                    case COMPLETED -> dao.casComplete(row.queryId());
                    case FAILED -> dao.casFail(row.queryId(), outcome.error());
                    case CANCELLED -> dao.casCancel(row.queryId());
                    case UNKNOWN -> {
                      // 状态以 manifest 为准（D7）：流断/not-found ≠失败，先对账
                      if (reconcile(row.queryId())) {
                        break;
                      }
                      // D36 抢跑宽限：预生成 opId 的 attach 可能早于 submit 到达引擎
                      // （"operation not found"类 UNKNOWN）——行龄 < orphan-grace 视为在途，
                      // cooldown 重试不误杀；≥ grace 仍 not-found 且无 manifest = 从未提交
                      // 成功，快败（原为 sweeper 600s）。
                      String error = outcome.error() == null ? "" : outcome.error();
                      if (error.startsWith("operation not found")
                          && row.createdAt() != null
                          && System.currentTimeMillis()
                                  - row.createdAt().toInstant().toEpochMilli()
                              >= config.getDurationMs("fg.attach.orphan-grace")) {
                        dao.casFail(row.queryId(), "operation not found: never submitted");
                        break;
                      }
                      attachRetryNotBefore.put(
                          row.queryId(),
                          System.currentTimeMillis() + attachRetryCooldownMs());
                      logger.info(
                          "Attach UNKNOWN for {} (cooldown {}ms before re-attach): {}",
                          row.queryId(),
                          attachRetryCooldownMs(),
                          error);
                    }
                  }
                } catch (Exception e) {
                  logger.error("Post-attach row migration failed for {}", row.queryId(), e);
                } finally {
                  // 先 CAS 后 release（顺序不可换——双挂竞态安全前提：release 切断他人流时
                  // 行已终态，被切流者的误判 CAS 撞终态行 no-op）
                  try {
                    engine.releaseExecution(
                        EngineExecutionHandle.parse(row.connectOperationId()));
                  } catch (Exception e) {
                    logger.debug("ReleaseExecution failed for {}: {}", row.queryId(), e.toString());
                  }
                  // 【remove 必须最后】若先 remove：完成与 poller 重建之间存在"map 空且
                  // cooldown 空/行未终态"窗口 → 无谓重建 attach（实证测试踩中）。先 CAS/
                  // cooldown 落定再摘除，醒来者要么见行终态、要么被 map/冷却拦下。
                  activeAttaches.remove(row.queryId(), future); // 带 value：不误删后来重建的
                }
              });
          return future;
        });
  }

  private long attachRetryCooldownMs() {
    return config.hasPath("fg.attach.retry.cooldown")
        ? config.getDurationMs("fg.attach.retry.cooldown")
        : config.getDurationMs("fg.poll.db.interval");
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
