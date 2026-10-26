package org.fg.dist;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.fg.common.BootStrapContext;
import org.fg.common.MetricsHttpServer;
import org.fg.common.concurrent.CloseableThreadPool;
import org.fg.common.concurrent.NamedThreadFactory;
import org.fg.common.config.GatewayConfig;
import org.fg.common.service.SingletonRegistry;
import org.fg.frontend.FgFlightService;
import org.fg.ha.FgZkClient;
import org.fg.orchestrator.QueryOrchestrator;
import org.fg.orchestrator.QueryTimeoutSweeper;
import org.fg.orchestrator.store.OperationStoreService;
import org.fg.result.store.ObjectStoreService;
import org.fg.result.store.ObjectStores;
import org.fg.spi.SqlEngine;
import org.fg.spark.client.SparkEngineClient;
import org.fg.spark.client.SparkEngineRouter;

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

    // D26：Prometheus 抓取端点（fg.metrics.enabled → BootStrapContext 已选 Prometheus 注册表）
    if (context.getMeterRegistry() instanceof io.micrometer.prometheusmetrics.PrometheusMeterRegistry) {
      registry.bindSelf(
          new MetricsHttpServer(
              (io.micrometer.prometheusmetrics.PrometheusMeterRegistry) context.getMeterRegistry(),
              config));
    }

    // 2. OperationStore（PostgreSQL + Flyway）
    OperationStoreService operationStore =
        new OperationStoreService(config, context.getMeterRegistry());
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
    // M2（D22）：fg.zk.addresses 非空 → ZK 发现 + share level 路由 + 拉起/恢复；
    // 空（默认）→ M1 固定单引擎（回归门：既有行为零改动）
    String zkAddresses = config.hasPath("fg.zk.addresses")
        ? config.getString("fg.zk.addresses").trim() : "";
    SqlEngine engine;
    if (!zkAddresses.isEmpty()) {
      FgZkClient zk = new FgZkClient(zkAddresses, config.getString("fg.zk.namespace"));
      zk.start();
      engine = new SparkEngineRouter(config, zk, context.getExecutor(), context.getMeterRegistry());
    } else {
      engine = new SparkEngineClient(config);
    }
    registry.bind(SqlEngine.class, engine);

    // 6. Orchestrator + 超时护栏
    QueryOrchestrator orchestrator =
        new QueryOrchestrator(
            operationStore.dao(), operationStore.sessionDao(), engine, objects, config,
            context.getExecutor());
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
            config, context.getAllocator(), orchestrator, objects, relayExecutor,
            context.getMeterRegistry());
    registry.bindSelf(flight);
  }
}
