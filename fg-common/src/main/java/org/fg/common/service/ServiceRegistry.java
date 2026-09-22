/*
 * Ported from Dremio OSS (common/legacy com.dremio.service.ServiceRegistry), Apache License 2.0.
 * Trimmed: no perf Timer / VM helpers.
 */
package org.fg.common.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import org.fg.common.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A simple service registry to start services in order and close them in reverse order. */
public class ServiceRegistry implements Service {
  private static final Logger logger = LoggerFactory.getLogger(ServiceRegistry.class);

  private volatile boolean closed = false;
  private final List<Service> services = new ArrayList<>();

  public <T extends Service> T register(T service) {
    if (service != null) {
      services.add(service);
    }
    return service;
  }

  public <T extends Service> T replace(T service) {
    if (service == null) {
      return null;
    }
    for (ListIterator<Service> it = services.listIterator(); it.hasNext(); ) {
      Service s = it.next();
      if (s.equals(service)) {
        it.remove();
        try {
          // Closing in case some resources are already allocated
          s.close();
        } catch (Exception e) {
          logger.warn("Exception when closing replaced service {}", s, e);
        }
        it.add(service);
        return service;
      }
    }
    throw new IllegalArgumentException("Trying to replace an unregistered service");
  }

  @Override
  public void start() throws Exception {
    for (Service service : services) {
      try {
        service.start();
      } catch (Exception e) {
        logger.error("Service {} failed to start", service, e);
        throw e;
      }
    }
  }

  @Override
  public synchronized void close() throws Exception {
    if (!closed) {
      closed = true;
      List<Service> reversed = new ArrayList<>(services);
      Collections.reverse(reversed);
      AutoCloseables.close(reversed);
    }
  }

  protected List<Service> getServices() {
    return services;
  }
}
