/*
 * Ported from Dremio OSS (common/legacy com.dremio.service.Service), Apache License 2.0.
 */
package org.fg.common.service;

/** Interface for Flight Gateway services. */
public interface Service extends AutoCloseable {
  void start() throws Exception;
}
