package org.fg.orchestrator.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.fg.common.service.Service;

/**
 * fg_operation DAO（design §4.2/F2/D14）。
 *
 * <p>关键语句形态：
 *
 * <ul>
 *   <li>幂等首 poll：INSERT ... ON CONFLICT (session_ref, sql_hash) DO NOTHING，0 行即已存在；
 *   <li>行迁移 CAS：UPDATE ... WHERE query_id=? AND status='RUNNING'——完成/取消双写者互斥。
 * </ul>
 *
 * <p>D36：attach 租约仲裁已移除;D38/V2 起 attach_owner/attach_lease_until 两列已删除。
 */
public class OperationStoreDao implements Service {

  private final DataSource dataSource;
  private final io.micrometer.core.instrument.MeterRegistry metrics;

  public OperationStoreDao(DataSource dataSource) {
    this(dataSource, new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
  }

  /** D26：注册/终态计数在此汇点（所有 CAS 路径——attach/对账/sweeper/cancel——统一覆盖）。 */
  public OperationStoreDao(
      DataSource dataSource, io.micrometer.core.instrument.MeterRegistry metrics) {
    this.dataSource = dataSource;
    this.metrics = metrics;
  }

  @Override
  public void start() {}

  @Override
  public void close() {}

  /** fg.query.registered{kind}——只计真实新建行（executeUpdate==1；poll 重注册不计）。 */
  private void countRegistered(String kind) {
    metrics.counter("fg.query.registered", "kind", kind).increment();
  }

  /** fg.query.outcome{outcome}——只计 CAS 赢得终态迁移（返回 true）的行。 */
  private void countOutcome(String outcome) {
    metrics.counter("fg.query.outcome", "outcome", outcome).increment();
  }

  /**
   * 幂等 INSERT；返回落库后的行（已存在时返回既有行）。schema_bytes 随行写入——
   * "行一出现 schema 即在"（调用方 register 在 INSERT 前同步 AnalyzePlan），消费方
   * 无需等待回填。
   *
   * <p>冲突推断按部分唯一索引（D19）：{@code ON CONFLICT (session_ref, sql_hash) WHERE
   * kind='QUERY'}——只对 QUERY 幂等；与 V1 的部分索引定义必须同提交（否则推断失败，QUERY
   * 注册即报 "no unique or exclusion constraint matching the ON CONFLICT specification"）。
   */
  public OperationRow insertOrGet(OperationRow row) throws SQLException {
    // D36：connect_operation_id 随 INSERT 落库（编排层预生成 UUID，Spark Connect 客户端可
    // 指定 operation_id）——行存在即可 attach/interrupt/release，消除"提交在途 opId 未知"窗口。
    String insert =
        "INSERT INTO fg_operation (query_id, session_ref, sql_hash, user_name, sql_text,"
            + " result_key_prefix, kind, mode, ordered, scrollable, schema_bytes, status,"
            + " connect_operation_id, engine_ref, created_at, updated_at)"
            + " VALUES (?, ?, ?, ?, ?, ?, 'QUERY', ?, ?, ?, ?, 'RUNNING', ?, ?, now(), now())"
            + " ON CONFLICT (session_ref, sql_hash) WHERE kind = 'QUERY' AND status = 'RUNNING' DO NOTHING";
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(insert)) {
      ps.setString(1, row.queryId());
      ps.setString(2, row.sessionRef());
      ps.setString(3, row.sqlHash());
      ps.setString(4, row.user());
      ps.setString(5, row.sqlText());
      ps.setString(6, row.resultKeyPrefix());
      ps.setString(7, row.mode().name());
      ps.setBoolean(8, row.ordered());
      ps.setBoolean(9, row.scrollable());
      ps.setBytes(10, row.schemaBytes());
      ps.setString(11, row.connectOperationId());
      ps.setString(12, row.engineRef());
      if (ps.executeUpdate() == 1) {
        countRegistered("QUERY");
        return row.status(OperationRow.Status.RUNNING);
      }
      return getByFingerprint(row.sessionRef(), row.sqlHash())
          .orElseThrow(() -> new IllegalStateException("Conflict but row missing: " + row.queryId()));
    }
  }

  /**
   * COMMAND 行插入（D18/D19 修正）：在途幂等——PollFlightInfo 逐次 register，无在途约束
   * 会令慢命令逐 poll 重复执行。冲突推断按部分唯一索引（V1 同提交）：
   * {@code ON CONFLICT (session_ref, sql_hash) WHERE kind='COMMAND' AND status='RUNNING'}
   * ——同 (session, sql) 同时至多一个在途命令，0 行即复用在途行；行终态后索引释放，
   * 下次执行照常新行（指纹幂等豁免 D19 保留）。schema_bytes 用静态宣告（或 analyzable
   * inline）schema 随行写入，终态由 {@link #casCompleteCommand} 以实际 schema+结果覆盖。
   */
  public OperationRow insertCommandOrGet(OperationRow row) throws SQLException {
    String insert =
        "INSERT INTO fg_operation (query_id, session_ref, sql_hash, user_name, sql_text,"
            + " result_key_prefix, kind, mode, ordered, scrollable, schema_bytes, status,"
            + " engine_ref, created_at, updated_at)"
            + " VALUES (?, ?, ?, ?, ?, ?, 'COMMAND', ?, ?, ?, ?, 'RUNNING', ?, now(), now())"
            + " ON CONFLICT (session_ref, sql_hash) WHERE kind = 'COMMAND'"
            + " AND status = 'RUNNING' DO NOTHING";
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(insert)) {
      ps.setString(1, row.queryId());
      ps.setString(2, row.sessionRef());
      ps.setString(3, row.sqlHash());
      ps.setString(4, row.user());
      ps.setString(5, row.sqlText());
      ps.setString(6, row.resultKeyPrefix());
      ps.setString(7, row.mode().name());
      ps.setBoolean(8, row.ordered());
      ps.setBoolean(9, row.scrollable());
      ps.setBytes(10, row.schemaBytes());
      ps.setString(11, row.engineRef());
      if (ps.executeUpdate() == 1) {
        countRegistered("COMMAND");
        return row.status(OperationRow.Status.RUNNING);
      }
      return getRunningCommand(row.sessionRef(), row.sqlHash())
          .orElseThrow(
              () -> new IllegalStateException("Command conflict but RUNNING row missing: " + row.queryId()));
    }
  }

  /** 在途 COMMAND 行查找（insertCommandOrGet 冲突路径用，D19 修正）。 */
  public Optional<OperationRow> getRunningCommand(String sessionRef, String sqlHash)
      throws SQLException {
    return queryOne(
        "SELECT * FROM fg_operation WHERE session_ref = ? AND sql_hash = ? AND kind = 'COMMAND'"
            + " AND status = 'RUNNING'",
        ps -> {
          ps.setString(1, sessionRef);
          ps.setString(2, sqlHash);
        });
  }

  /**
   * CAS 命令完成（D18）：RUNNING→COMPLETED，落实际 schema（覆盖静态宣告）+ 内联结果
   * （Arrow IPC stream bytes）。
   */
  public boolean casCompleteCommand(String queryId, byte[] schemaBytes, byte[] resultBytes)
      throws SQLException {
    boolean won =
        update(
            "UPDATE fg_operation SET status = 'COMPLETED', schema_bytes = ?, command_result = ?,"
                + " terminal_at = now(), updated_at = now()"
                + " WHERE query_id = ? AND status = 'RUNNING'",
            ps -> {
              ps.setBytes(1, schemaBytes);
              ps.setBytes(2, resultBytes);
              ps.setString(3, queryId);
            });
    if (won) {
      countOutcome("COMPLETED");
    }
    return won;
  }

  public Optional<OperationRow> get(String queryId) throws SQLException {
    return queryOne(
        "SELECT * FROM fg_operation WHERE query_id = ?", ps -> ps.setString(1, queryId));
  }

  /**
   * 指纹查找（D19 防御：只看 QUERY 行；D28 修订：RUNNING-only 唯一索引后同指纹可存在
   * 多个终态行——取最新，无头路径的"终态复用"语义保持）。RUNNING 中的行天然比任何终态行新。
   */
  public Optional<OperationRow> getByFingerprint(String sessionRef, String sqlHash)
      throws SQLException {
    return queryOne(
        "SELECT * FROM fg_operation WHERE session_ref = ? AND sql_hash = ? AND kind = 'QUERY'"
            + " ORDER BY created_at DESC, query_id DESC LIMIT 1",
        ps -> {
          ps.setString(1, sessionRef);
          ps.setString(2, sqlHash);
        });
  }

  /**
   * D28 续传：按 (session, queryId) 精确查行——x-fg-query-id 头的命中判定（①）。
   * queryId 为不透明串（客户端铸造），任何状态都可能（RUNNING=poll 收敛、终态=续传）。
   */
  public Optional<OperationRow> getBySessionAndQueryId(String sessionRef, String queryId)
      throws SQLException {
    return queryOne(
        "SELECT * FROM fg_operation WHERE session_ref = ? AND query_id = ?",
        ps -> {
          ps.setString(1, sessionRef);
          ps.setString(2, queryId);
        });
  }

  /**
   * D39：submit 确认——引擎首响应 operationId 回显后落 {@code submitted_at}（顺带回填
   * connect_operation_id 幂等兜底，滚动升级期旧版本行）。单一写点（submit/submitInline/
   * executeCommand 三回调），poll/attach 以此门控。无条件写：终态行也写（留证无害）。
   */
  public boolean markSubmitted(String queryId, String engineHandle) throws SQLException {
    return update(
        "UPDATE fg_operation SET submitted_at = now(), connect_operation_id = ?,"
            + " updated_at = now() WHERE query_id = ?",
        ps -> {
          ps.setString(1, engineHandle);
          ps.setString(2, queryId);
        });
  }

  public boolean setSchema(String queryId, byte[] schemaBytes) throws SQLException {
    return update(
        "UPDATE fg_operation SET schema_bytes = ?, updated_at = now() WHERE query_id = ?",
        ps -> {
          ps.setBytes(1, schemaBytes);
          ps.setString(2, queryId);
        });
  }

  public boolean setOrdered(String queryId, boolean ordered) throws SQLException {
    return update(
        "UPDATE fg_operation SET ordered = ?, updated_at = now() WHERE query_id = ?",
        ps -> {
          ps.setBoolean(1, ordered);
          ps.setString(2, queryId);
        });
  }

  /** CAS 完成（attach 者或对账修复者）。 */
  public boolean casComplete(String queryId) throws SQLException {
    return casTerminal(queryId, "COMPLETED", null);
  }

  public boolean casFail(String queryId, String error) throws SQLException {
    return casTerminal(queryId, "FAILED", error);
  }

  /** CAS 取消（取消服务实例；与完成写者互斥——0 行命中=对方已迁终态）。 */
  public boolean casCancel(String queryId) throws SQLException {
    return casTerminal(queryId, "CANCELLED", null);
  }

  private boolean casTerminal(String queryId, String status, String error) throws SQLException {
    boolean won =
        update(
            "UPDATE fg_operation SET status = ?, error = ?, terminal_at = now(), updated_at = now()"
                + " WHERE query_id = ? AND status = 'RUNNING'",
            ps -> {
              ps.setString(1, status);
              ps.setString(2, error);
              ps.setString(3, queryId);
            });
    if (won) {
      countOutcome(status);
    }
    return won;
  }

  /** 超时护栏（D9）：RUNNING 且 created_at 早于 cutoff 的行。 */
  public List<OperationRow> findRunningCreatedBefore(OffsetDateTime cutoff) throws SQLException {
    return queryList(
        "SELECT * FROM fg_operation WHERE status = 'RUNNING' AND created_at < ?",
        ps -> ps.setObject(1, cutoff));
  }

  /** retention 同步清扫：终态早于 cutoff 的行 id。 */
  public List<String> findTerminalBefore(OffsetDateTime cutoff) throws SQLException {
    String sql = "SELECT query_id FROM fg_operation WHERE terminal_at IS NOT NULL AND terminal_at < ?";
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setObject(1, cutoff);
      try (ResultSet rs = ps.executeQuery()) {
        List<String> ids = new ArrayList<>();
        while (rs.next()) {
          ids.add(rs.getString(1));
        }
        return ids;
      }
    }
  }

  public boolean delete(String queryId) throws SQLException {
    return update(
        "DELETE FROM fg_operation WHERE query_id = ?", ps -> ps.setString(1, queryId));
  }

  // ------------------------------------------------------------------ helpers

  private boolean update(String sql, Binder binder) throws SQLException {
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      binder.bind(ps);
      return ps.executeUpdate() > 0;
    }
  }

  private Optional<OperationRow> queryOne(String sql, Binder binder) throws SQLException {
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      binder.bind(ps);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(map(rs)) : Optional.empty();
      }
    }
  }

  private List<OperationRow> queryList(String sql, Binder binder) throws SQLException {
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      binder.bind(ps);
      try (ResultSet rs = ps.executeQuery()) {
        List<OperationRow> rows = new ArrayList<>();
        while (rs.next()) {
          rows.add(map(rs));
        }
        return rows;
      }
    }
  }

  static OperationRow map(ResultSet rs) throws SQLException {
    return new OperationRow()
        .queryId(rs.getString("query_id"))
        .sessionRef(rs.getString("session_ref"))
        .sqlHash(rs.getString("sql_hash"))
        .user(rs.getString("user_name"))
        .sqlText(rs.getString("sql_text"))
        .resultKeyPrefix(rs.getString("result_key_prefix"))
        .kind(OperationRow.Kind.parse(rs.getString("kind")))
        .commandResult(rs.getBytes("command_result"))
        .mode(OperationRow.Mode.parse(rs.getString("mode")))
        .ordered(rs.getBoolean("ordered"))
        .scrollable(rs.getBoolean("scrollable"))
        .schemaBytes(rs.getBytes("schema_bytes"))
        .status(OperationRow.Status.valueOf(rs.getString("status")))
        .connectOperationId(rs.getString("connect_operation_id"))
        .submittedAt(rs.getObject("submitted_at", OffsetDateTime.class))
        .engineRef(rs.getString("engine_ref"))
        .error(rs.getString("error"))
        .terminalAt(rs.getObject("terminal_at", OffsetDateTime.class))
        .createdAt(rs.getObject("created_at", OffsetDateTime.class))
        .updatedAt(rs.getObject("updated_at", OffsetDateTime.class));
  }

  interface Binder {
    void bind(PreparedStatement ps) throws SQLException;
  }
}
