package org.fg.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.fg.orchestrator.store.OperationRow;
import org.fg.orchestrator.store.OperationStoreDao;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * OperationStore DAO 集成测试（design §4.2/D14，需 Docker）：DDL 迁移、幂等 INSERT、CAS
 * 互斥（两写者仅一成功）、attach 租约仲裁。
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OperationStoreDaoIT {

  @Container
  private static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withDatabaseName("fg")
          .withUsername("fg")
          .withPassword("fg");

  private static HikariDataSource ds;
  private static OperationStoreDao dao;

  @BeforeAll
  static void setUp() {
    HikariConfig hikari = new HikariConfig();
    hikari.setJdbcUrl(PG.getJdbcUrl());
    hikari.setUsername(PG.getUsername());
    hikari.setPassword(PG.getPassword());
    hikari.setMaximumPoolSize(4);
    ds = new HikariDataSource(hikari);
    Flyway.configure().dataSource(ds).locations("classpath:db/migration").load().migrate();
    dao = new OperationStoreDao(ds);
  }

  @AfterAll
  static void tearDown() {
    if (ds != null) {
      ds.close();
    }
  }

  private OperationRow newRow(String sessionRef, String sql) {
    String queryId = UUID.randomUUID().toString();
    return new OperationRow()
        .queryId(queryId)
        .sessionRef(sessionRef)
        .sqlHash(QueryOrchestrator.sha256(sql))
        .user("alice")
        .sqlText(sql)
        .resultKeyPrefix("results/spark/alice/" + queryId)
        .mode(OperationRow.Mode.RELAY)
        .status(OperationRow.Status.RUNNING);
  }

  @Test
  void idempotentInsertOnFingerprintConflict() throws Exception {
    OperationRow first = dao.insertOrGet(newRow("s1", "SELECT 1"));
    OperationRow second = dao.insertOrGet(newRow("s1", "SELECT 1"));
    assertThat(second.queryId()).isEqualTo(first.queryId()); // 网络重试不重复执行
    OperationRow otherSession = dao.insertOrGet(newRow("s2", "SELECT 1"));
    assertThat(otherSession.queryId()).isNotEqualTo(first.queryId());
  }

  @Test
  void casCompletionAndCancelAreMutuallyExclusive() throws Exception {
    OperationRow row = dao.insertOrGet(newRow("s-cas", "SELECT cas"));
    assertThat(dao.casComplete(row.queryId())).isTrue();
    assertThat(dao.casCancel(row.queryId())).isFalse(); // 已终态，取消撞空
    assertThat(dao.get(row.queryId()).orElseThrow().status())
        .isEqualTo(OperationRow.Status.COMPLETED);

    OperationRow row2 = dao.insertOrGet(newRow("s-cas-2", "SELECT cas2"));
    assertThat(dao.casCancel(row2.queryId())).isTrue();
    assertThat(dao.casComplete(row2.queryId())).isFalse(); // 取消写者已迁移
  }

  @Test
  void concurrentCasOnlyOneWinner() throws Exception {
    OperationRow row = dao.insertOrGet(newRow("s-race", "SELECT race"));
    int threads = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<Boolean>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        final boolean complete = i % 2 == 0;
        futures.add(
            pool.submit(
                () -> {
                  start.await();
                  return complete ? dao.casComplete(row.queryId()) : dao.casCancel(row.queryId());
                }));
      }
      start.countDown();
      int successes = 0;
      for (Future<Boolean> f : futures) {
        if (f.get()) {
          successes++;
        }
      }
      assertThat(successes).isEqualTo(1); // CAS 互斥：仅一写者成功
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void attachLeaseArbitrationAndTakeover() throws Exception {
    OperationRow row = dao.insertOrGet(newRow("s-lease", "SELECT lease"));
    assertThat(dao.tryAcquireAttachLease(row.queryId(), "owner-a", Duration.ofSeconds(60))).isTrue();
    assertThat(dao.tryAcquireAttachLease(row.queryId(), "owner-b", Duration.ofSeconds(60))).isFalse();
    assertThat(dao.tryAcquireAttachLease(row.queryId(), "owner-a", Duration.ofSeconds(60))).isTrue(); // 本人续持
    // 租约过期接管：直接用过期租约回填模拟
    try (var c = ds.getConnection();
        var ps = c.prepareStatement(
            "UPDATE fg_operation SET attach_lease_until = now() - interval '1 second' WHERE query_id = ?")) {
      ps.setObject(1, UUID.fromString(row.queryId()));
      ps.executeUpdate();
    }
    assertThat(dao.tryAcquireAttachLease(row.queryId(), "owner-b", Duration.ofSeconds(60))).isTrue();
    assertThat(dao.clearAttach(row.queryId(), "owner-b")).isTrue();
  }
}
