package org.fg.dist;

import org.fg.common.BootStrapContext;
import org.fg.common.config.GatewayConfig;
import org.fg.common.service.SingletonRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gateway 启动入口（design §5，DACDaemon 范式：registry 顺序 start、逆序 close、shutdownHook）。
 */
public final class GatewayDaemon {

  private static final Logger logger = LoggerFactory.getLogger(GatewayDaemon.class);

  private final SingletonRegistry registry = new SingletonRegistry();
  private final Object shutdownLock = new Object();
  private boolean closed = false;

  public GatewayDaemon() throws Exception {
    // 1. Config/BootStrapContext
    GatewayConfig config = GatewayConfig.create();
    BootStrapContext context = new BootStrapContext(config);
    registry.bind(BootStrapContext.class, context);

    // 2..8 各服务
    GatewayDaemonModule.build(registry, context);
  }

  public void start() throws Exception {
    registry.start();
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    close();
                  } catch (Exception e) {
                    logger.error("Shutdown failed", e);
                  }
                },
                "fg-shutdown-hook"));
    logger.info("Flight Gateway started (flight port {})",
        GatewayConfig.create().getInt(GatewayConfig.FLIGHT_PORT));
  }

  public void close() throws Exception {
    synchronized (shutdownLock) {
      if (closed) {
        return;
      }
      closed = true;
    }
    registry.close();
  }

  public static void main(String[] args) throws Exception {
    GatewayDaemon daemon = new GatewayDaemon();
    daemon.start();
    // 阻塞主线程（服务线程均为 daemon）
    Thread.currentThread().join();
  }
}
