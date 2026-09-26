package org.fg.orchestrator.store;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.fg.common.service.Service;

/**
 * fg_session DAO（design D20 生命周期绑定）。fg 会话 ↔ 引擎 Connect 会话一一对应；化身
 * （connect_session_id）由本表铸造且唯一裁决。CLOSED 为终态、sticky（markClosed 单向
 * CAS）——不复活；客户端换新会话身份（重连取新 cookie / 轮换 x-fg-session-id）。
 *
 * <p>会话选项不落盘：值在引擎会话 conf 随会话生灭，本表仅登记 option_keys 供
 * GetSessionOptions 回读（mergeOptionKeys 的 add/remove 由 SetSessionOptions 的设/清驱动）。
 */
public class SessionRegistryDao implements Service {

  /** 会话状态：ACTIVE 在役；CLOSED 终态（closed_reason: client | engine_lost）。 */
  public enum Status {
    ACTIVE,
    CLOSED;

    static Status parse(String v) {
      return "CLOSED".equals(v) ? CLOSED : ACTIVE;
    }
  }

  /** fg_session 行（不可变快照；迁移经 DAO 条件更新完成）。 */
  public record SessionRow(
      String sessionRef,
      String user,
      String engineRef,
      String connectSessionId,
      Long engineStartedAt,
      List<String> optionKeys,
      Status status,
      String closedReason) {}

  /** bornOrGet 结果：isNew=false 表示并发/重复接触命中既有行。 */
  public record Born(SessionRow row, boolean isNew) {}

  private final DataSource dataSource;

  public SessionRegistryDao(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void start() {}

  @Override
  public void close() {}

  /**
   * 首次接触登记（mint-on-first-contact）：INSERT ... ON CONFLICT DO NOTHING——铸造
   * connect_session_id（随机 UUID，化身自此由本表唯一裁决）；0 行即既有会话，选回。
   * 与 fg_operation.insertOrGet 同款多实例仲裁。
   */
  public Born bornOrGet(String sessionRef, String user, String engineRef) throws SQLException {
    String insert =
        "INSERT INTO fg_session (session_ref, user_name, engine_ref, connect_session_id)"
            + " VALUES (?, ?, ?, ?) ON CONFLICT (session_ref) DO NOTHING";
    UUID minted = UUID.randomUUID();
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(insert)) {
      ps.setString(1, sessionRef);
      ps.setString(2, user);
      ps.setString(3, engineRef);
      ps.setObject(4, minted);
      if (ps.executeUpdate() == 1) {
        return new Born(
            new SessionRow(sessionRef, user, engineRef, minted.toString(), null,
                List.of(), Status.ACTIVE, null),
            true);
      }
    }
    return new Born(
        get(sessionRef)
            .orElseThrow(() -> new IllegalStateException("Session conflict but row missing: " + sessionRef)),
        false);
  }

  public Optional<SessionRow> get(String sessionRef) throws SQLException {
    String sql = "SELECT * FROM fg_session WHERE session_ref = ?";
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, sessionRef);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(map(rs)) : Optional.empty();
      }
    }
  }

  /** 活跃度心跳（仅 ACTIVE 行；CLOSED 不复活）。 */
  public void touch(String sessionRef) throws SQLException {
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(
            "UPDATE fg_session SET last_active_at = now()"
                + " WHERE session_ref = ? AND status = 'ACTIVE'")) {
      ps.setString(1, sessionRef);
      ps.executeUpdate();
    }
  }

  /** 记录引擎侧化身起点（首次 attach 校验发现 alive 时；仅空值可写）。 */
  public void markEngineStarted(String sessionRef, long engineStartedAtMs) throws SQLException {
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(
            "UPDATE fg_session SET engine_started_at = ?"
                + " WHERE session_ref = ? AND status = 'ACTIVE' AND engine_started_at IS NULL")) {
      ps.setLong(1, engineStartedAtMs);
      ps.setString(2, sessionRef);
      ps.executeUpdate();
    }
  }

  /** 终态迁移（单向 CAS）：client（显式关闭）| engine_lost（attach 校验发现引擎侧已死）。 */
  public boolean markClosed(String sessionRef, String reason) throws SQLException {
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(
            "UPDATE fg_session SET status = 'CLOSED', closed_reason = ?, closed_at = now()"
                + " WHERE session_ref = ? AND status = 'ACTIVE'")) {
      ps.setString(1, reason);
      ps.setString(2, sessionRef);
      return ps.executeUpdate() == 1;
    }
  }

  /**
   * 选项键登记（值为引擎会话态不落盘）：add 并入、remove 剔除，DISTINCT 保序由引擎侧
   * 回读排序兜底。空 add/remove 均安全（COALESCE 防 ANY(空数组) 的 NULL 三值逻辑）。
   */
  public void mergeOptionKeys(
      String sessionRef, Collection<String> add, Collection<String> remove) throws SQLException {
    String sql =
        "UPDATE fg_session SET option_keys ="
            + " (SELECT COALESCE(array_agg(DISTINCT k), '{}'::text[]) FROM unnest(option_keys || ?::text[]) AS k"
            + "  WHERE NOT COALESCE(k = ANY(?::text[]), false))"
            + " WHERE session_ref = ? AND status = 'ACTIVE'";
    try (Connection c = dataSource.getConnection();
        PreparedStatement ps = c.prepareStatement(sql)) {
      Array addArr = c.createArrayOf("text", add.toArray(new String[0]));
      Array removeArr = c.createArrayOf("text", remove.toArray(new String[0]));
      ps.setArray(1, addArr);
      ps.setArray(2, removeArr);
      ps.setString(3, sessionRef);
      ps.executeUpdate();
      addArr.free();
      removeArr.free();
    }
  }

  private static SessionRow map(ResultSet rs) throws SQLException {
    Array keys = rs.getArray("option_keys");
    List<String> optionKeys = new ArrayList<>();
    if (keys != null) {
      try {
        optionKeys.addAll(List.of((String[]) keys.getArray()));
      } finally {
        keys.free();
      }
    }
    Long started = rs.getObject("engine_started_at") == null ? null : rs.getLong("engine_started_at");
    return new SessionRow(
        rs.getString("session_ref"),
        rs.getString("user_name"),
        rs.getString("engine_ref"),
        rs.getObject("connect_session_id", UUID.class).toString(),
        started,
        List.copyOf(optionKeys),
        Status.parse(rs.getString("status")),
        rs.getString("closed_reason"));
  }
}
