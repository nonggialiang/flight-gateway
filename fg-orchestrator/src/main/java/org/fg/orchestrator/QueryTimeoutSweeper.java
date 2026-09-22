package org.fg.orchestrator;

import java.time.OffsetDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.fg.common.concurrent.NamedThreadFactory;
import org.fg.common.config.GatewayConfig;
import org.fg.common.service.Service;
import org.fg.orchestrator.store.OperationStoreDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 查询超时护栏（design D9）：fg.query.timeout 是唯一护栏（无字节熔断）。RUNNING 且超过
 * timeout 的行迁 FAILED。retention 行清扫在此一并调度（fg.result.retention）。
 */
public class QueryTimeoutSweeper implements Service {

  private static final Logger logger = LoggerFactory.getLogger(QueryTimeoutSweeper.class);

  private final OperationStoreDao dao;
  private final GatewayConfig config;
  private ScheduledExecutorService scheduler;

  public QueryTimeoutSweeper(OperationStoreDao dao, GatewayConfig config) {
    this.dao = dao;
    this.config = config;
  }

  @Override
  public void start() {
    scheduler =
        Executors.newScheduledThreadPool(2, new NamedThreadFactory("fg-housekeeping-"));
    scheduler.scheduleWithFixedDelay(this::sweepTimeouts, 30, 30, TimeUnit.SECONDS);
  }

  @Override
  public void close() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }

  void sweepTimeouts() {
    try {
      long timeoutMs = config.getDurationMs("fg.query.timeout");
      dao.findRunningCreatedBefore(OffsetDateTime.now().minusSeconds(timeoutMs / 1000))
          .forEach(
              row -> {
                try {
                  if (dao.casFail(row.queryId(), "query timeout after " + timeoutMs + "ms")) {
                    logger.info("Query {} timed out", row.queryId());
                  }
                } catch (Exception e) {
                  logger.warn("Timeout sweep failed for {}: {}", row.queryId(), e.toString());
                }
              });
    } catch (Exception e) {
      logger.warn("Timeout sweep iteration failed: {}", e.toString());
    }
  }
}
