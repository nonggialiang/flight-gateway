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

  private final GatewayConfig config;
  private HikariDataSource dataSource;
  private OperationStoreDao dao;

  public OperationStoreService(GatewayConfig config) {
    this.config = config;
  }

  @Override
  public void start() {
    HikariConfig hikari = new HikariConfig();
    hikari.setJdbcUrl(config.getString("fg.db.url"));
    hikari.setUsername(config.getString("fg.db.username"));
    hikari.setPassword(config.getString("fg.db.password"));
    hikari.setMaximumPoolSize(config.getInt("fg.db.max.pool.size"));
    hikari.setPoolName("fg-operation-store");
    this.dataSource = new HikariDataSource(hikari);

    Flyway.configure()
        .dataSource(dataSource)
        .locations("classpath:db/migration")
        .load()
        .migrate();

    this.dao = new OperationStoreDao(dataSource);
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
}
