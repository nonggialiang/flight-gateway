package org.fg.orchestrator.store;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.fg.common.config.GatewayConfig;
import org.fg.common.service.Service;
import org.flywaydb.core.Flyway;

/**
 * OperationStore 服务：HikariCP 连接池 + Flyway 迁移（design D14）。启动顺序上先于一切业务
 * 服务（GatewayDaemon 中位于引擎/编排之前）。
 */
public final class OperationStoreService implements Service {

  private final HikariDataSource dataSource;
  private final OperationStoreDao dao;
  private final SessionRegistryDao sessionDao;

  public OperationStoreService(GatewayConfig config) {
    HikariConfig hikari = new HikariConfig();
    hikari.setJdbcUrl(config.getString("fg.db.url"));
    hikari.setUsername(config.getString("fg.db.username"));
    hikari.setPassword(config.getString("fg.db.password"));
    hikari.setMaximumPoolSize(config.getInt("fg.db.max.pool.size"));
    hikari.setPoolName("fg-operation-store");
    this.dataSource = new HikariDataSource(hikari);
    this.dao = new OperationStoreDao(dataSource);
    this.sessionDao = new SessionRegistryDao(dataSource);
  }

  @Override
  public void start() {
    // 迁移在启动期执行（DAO 已在构造器就绪，供装配期获取）
    Flyway.configure()
        .dataSource(dataSource)
        .locations("classpath:db/migration")
        .load()
        .migrate();
  }

  @Override
  public void close() {
    if (dataSource != null) {
      dataSource.close();
    }
  }

  public DataSource dataSource() {
    return dataSource;
  }

  public OperationStoreDao dao() {
    return dao;
  }

  /** fg_session 会话登记 DAO（D20 生命周期绑定）。 */
  public SessionRegistryDao sessionDao() {
    return sessionDao;
  }
}
