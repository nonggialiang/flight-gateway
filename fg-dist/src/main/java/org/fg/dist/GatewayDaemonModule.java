package org.fg.dist;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.fg.common.BootStrapContext;
import org.fg.common.concurrent.CloseableThreadPool;
import org.fg.common.concurrent.NamedThreadFactory;
import org.fg.common.config.GatewayConfig;
import org.fg.common.service.SingletonRegistry;
import org.fg.frontend.FgFlightService;
import org.fg.orchestrator.QueryOrchestrator;
import org.fg.orchestrator.QueryTimeoutSweeper;
import org.fg.orchestrator.store.OperationStoreService;
import org.fg.result.store.ObjectStoreService;
import org.fg.result.store.ObjectStores;
import org.fg.spi.SqlEngine;
import org.fg.spark.client.SparkEngineClient;

/**
 * 装配模块（design §5/DACDaemonModule 范式）。
 *
 * <p>启动顺序：Config/BootStrapContext → OperationStore → ResultStore(ObjectStore) → Engine
 * (Kit) → Orchestrator → 超时护栏 → FlightFrontend。Kit 装配点在此——A/B 路线回退开关
 * {@code fg.engine.spark.route}（M1 仅实现 B；失败回退时替换为路线 A 的 Kit 内实现，SPI 不变）。
 */
final class GatewayDaemonModule {

  private GatewayDaemonModule() {}

  static void build(SingletonRegistry registry, BootStrapContext context) {
    GatewayConfig config = context.getConfig();

    // 2. OperationStore（PostgreSQL + Flyway）
    OperationStoreService operationStore = new OperationStoreService(config);
    registry.bind(OperationStoreService.class, operationStore);

    // 4. ResultStore（presign/manifest/purge）
    ObjectStoreService objects = ObjectStores.fromConfig(config);
    registry.bind(ObjectStoreService.class, objects);

    // 5. Engine Kit（路线 B：Connect 薄 stub；A/B 开关：fg.engine.spark.route）
    String route = config.hasPath("fg.engine.spark.route")
        ? config.getString("fg.engine.spark.route") : "B";
    if (!"B".equalsIgnoreCase(route)) {
      throw new IllegalStateException(
          "fg.engine.spark.route=A (fallback) lands with PoC failure; M1 ships route B only");
    }
    SqlEngine engine = new SparkEngineClient(config);
    registry.bind(SqlEngine.class, engine);

    // 6. Orchestrator + 超时护栏
    QueryOrchestrator orchestrator =
        new QueryOrchestrator(
            operationStore.dao(), engine, objects, config, context.getExecutor());
    registry.bindSelf(orchestrator);
    registry.bindSelf(new QueryTimeoutSweeper(operationStore.dao(), config));

    // relay 有界池（fg.executor.relay.size；SynchronousQueue=真有界并发）
    int relaySize = config.getInt("fg.executor.relay.size");
    CloseableThreadPool relayExecutor =
        new CloseableThreadPool(
            "fg-relay-", relaySize, relaySize, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
    registry.bind(CloseableThreadPool.class, relayExecutor);

    // 8. FlightFrontend（对外）
    FgFlightService flight =
        new FgFlightService(
            config, context.getAllocator(), orchestrator, objects, relayExecutor);
    registry.bindSelf(flight);
  }
}
