package org.fg.orchestrator.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.fg.common.service.Service;

/**
 * fg_operation DAO（design §4.2/F2/D14）。
 *
 * <p>关键语句形态：
 *
 * <ul>
 *   <li>幂等首 poll：INSERT ... ON CONFLICT (session_ref, sql_hash) DO NOTHING，0 行即已存在；
 *   <li>行迁移 CAS：UPDATE ... WHERE query_id=? AND status='RUNNING'——完成/取消双写者互斥；
 *   <li>attach 租约仲裁：条件 UPDATE（owner 为空/过期/本人可获），租约过期可接管。
 * </ul>
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
    String insert =
        "INSERT INTO fg_operation (query_id, session_ref, sql_hash, user_name, sql_text,"
            + " result_key_prefix, kind, mode, ordered, scrollable, schema_bytes, status,"
            + " engine_ref, created_at, updated_at)"
            + " VALUES (?, ?, ?, ?, ?, ?, 'QUERY', ?, ?, ?, ?, 'RUNNING', ?, now(), now())"
            + " ON CONFLICT (session_ref, sql_hash) WHERE kind = 'QUERY' DO NOTHING";
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(insert)) {
      ps.setObject(1, UUID.fromString(row.queryId()));
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
      ps.setObject(1, UUID.fromString(row.queryId()));
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
                + " terminal_at = now(), updated_at = now(), attach_owner = NULL, attach_lease_until = NULL"
                + " WHERE query_id = ? AND status = 'RUNNING'",
            ps -> {
              ps.setBytes(1, schemaBytes);
              ps.setBytes(2, resultBytes);
              ps.setObject(3, UUID.fromString(queryId));
            });
    if (won) {
      countOutcome("COMPLETED");
    }
    return won;
  }

  public Optional<OperationRow> get(String queryId) throws SQLException {
    return queryOne(
        "SELECT * FROM fg_operation WHERE query_id = ?", ps -> ps.setObject(1, UUID.fromString(queryId)));
  }

  /** 指纹查找只看 QUERY 行（D19 防御：COMMAND 行不受唯一索引约束，同名指纹可多行）。 */
  public Optional<OperationRow> getByFingerprint(String sessionRef, String sqlHash)
      throws SQLException {
    return queryOne(
        "SELECT * FROM fg_operation WHERE session_ref = ? AND sql_hash = ? AND kind = 'QUERY'",
        ps -> {
          ps.setString(1, sessionRef);
          ps.setString(2, sqlHash);
        });
  }

  /** 触发实例首响应捕获 operationId 后回填（H1 映射）。 */
  public boolean setOperationId(String queryId, String engineHandle) throws SQLException {
    return update(
        "UPDATE fg_operation SET connect_operation_id = ?, updated_at = now() WHERE query_id = ?",
        ps -> {
          ps.setString(1, engineHandle);
          ps.setObject(2, UUID.fromString(queryId));
        });
  }

  public boolean setSchema(String queryId, byte[] schemaBytes) throws SQLException {
    return update(
        "UPDATE fg_operation SET schema_bytes = ?, updated_at = now() WHERE query_id = ?",
        ps -> {
          ps.setBytes(1, schemaBytes);
          ps.setObject(2, UUID.fromString(queryId));
        });
  }

  public boolean setOrdered(String queryId, boolean ordered) throws SQLException {
    return update(
        "UPDATE fg_operation SET ordered = ?, updated_at = now() WHERE query_id = ?",
        ps -> {
          ps.setBoolean(1, ordered);
          ps.setObject(2, UUID.fromString(queryId));
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
            "UPDATE fg_operation SET status = ?, error = ?, terminal_at = now(), updated_at = now(),"
                + " attach_owner = NULL, attach_lease_until = NULL"
                + " WHERE query_id = ? AND status = 'RUNNING'",
            ps -> {
              ps.setString(1, status);
              ps.setString(2, error);
              ps.setObject(3, UUID.fromString(queryId));
            });
    if (won) {
      countOutcome(status);
    }
    return won;
  }

  /**
   * attach 租约仲裁：owner 为空/已过期/本人续持 时获得（赢家 true）。行 RUNNING 才可 attach。
   */
  public boolean tryAcquireAttachLease(String queryId, String ownerId, Duration lease)
      throws SQLException {
    return update(
        "UPDATE fg_operation SET attach_owner = ?, attach_lease_until = now() + ?::interval,"
            + " updated_at = now()"
            + " WHERE query_id = ? AND status = 'RUNNING'"
            + " AND (attach_owner IS NULL OR attach_owner = ? OR attach_lease_until < now())",
        ps -> {
          ps.setString(1, ownerId);
          ps.setString(2, toPostgresInterval(lease));
          ps.setObject(3, UUID.fromString(queryId));
          ps.setString(4, ownerId);
        });
  }

  /** attach 者主动释放租约（终态后清理）。 */
  public boolean clearAttach(String queryId, String ownerId) throws SQLException {
    return update(
        "UPDATE fg_operation SET attach_owner = NULL, attach_lease_until = NULL, updated_at = now()"
            + " WHERE query_id = ? AND attach_owner = ?",
        ps -> {
          ps.setObject(1, UUID.fromString(queryId));
          ps.setString(2, ownerId);
        });
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
          ids.add(rs.getObject(1, UUID.class).toString());
        }
        return ids;
      }
    }
  }

  public boolean delete(String queryId) throws SQLException {
    return update(
        "DELETE FROM fg_operation WHERE query_id = ?", ps -> ps.setObject(1, UUID.fromString(queryId)));
  }

  // ------------------------------------------------------------------ helpers

  private static String toPostgresInterval(Duration d) {
    return d.toMillis() + " milliseconds";
  }

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
        .queryId(rs.getObject("query_id", UUID.class).toString())
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
        .attachOwner(rs.getString("attach_owner"))
        .attachLeaseUntil(rs.getObject("attach_lease_until", OffsetDateTime.class))
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
