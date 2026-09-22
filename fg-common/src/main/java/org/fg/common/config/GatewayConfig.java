/*
 * Ported from Dremio OSS (common/legacy com.dremio.config.DremioConfig), Apache License 2.0.
 * Trimmed: no SabotConfig / legacy system properties. Load order:
 * fg-reference.conf (classpath) <- gateway.conf (classpath or file) <- -Dfg.* system properties.
 */
package org.fg.common.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import java.util.List;

/** Gateway configuration, DremioConfig-style three-layer merge. */
public final class GatewayConfig {

  public static final String REFERENCE_CONF = "fg-reference.conf";
  public static final String CONFIG_FILE = "gateway.conf";

  private final Config effective;

  private GatewayConfig(Config effective) {
    this.effective = effective;
  }

  public static GatewayConfig create() {
    final Config reference = ConfigFactory.parseResources(REFERENCE_CONF);
    final Config user = ConfigFactory.parseResources(CONFIG_FILE).withFallback(reference);
    return new GatewayConfig(applySystemProperties(user).resolve());
  }

  /**
   * Applies {@code -Dfg.*} system properties as overrides. Values are parsed as boolean / long /
   * double / comma-list / string.
   */
  static Config applySystemProperties(Config config) {
    Config result = config;
    for (String propName : System.getProperties().stringPropertyNames()) {
      if (!propName.startsWith("fg.")) {
        continue;
      }
      final String value = System.getProperty(propName);
      result = result.withValue(propName, toConfigValue(value));
    }
    return result;
  }

  private static ConfigValue toConfigValue(String value) {
    if ("true".equals(value) || "false".equals(value)) {
      return ConfigValueFactory.fromAnyRef(Boolean.parseBoolean(value));
    }
    try {
      return ConfigValueFactory.fromAnyRef(Long.parseLong(value));
    } catch (NumberFormatException ignored) {
      // fall through
    }
    try {
      return ConfigValueFactory.fromAnyRef(Double.parseDouble(value));
    } catch (NumberFormatException ignored) {
      // fall through
    }
    if (value.contains(",")) {
      return ConfigValueFactory.fromAnyRef(List.of(value.split(",")));
    }
    return ConfigValueFactory.fromAnyRef(value);
  }

  public boolean hasPath(String path) {
    return effective.hasPath(path);
  }

  public String getString(String path) {
    return effective.getString(path);
  }

  public int getInt(String path) {
    return effective.getInt(path);
  }

  public long getLong(String path) {
    return effective.getLong(path);
  }

  public boolean getBoolean(String path) {
    return effective.getBoolean(path);
  }

  public long getBytes(String path) {
    return effective.getBytes(path);
  }

  public long getDurationMs(String path) {
    return effective.getDuration(path).toMillis();
  }

  public GatewayConfig getConfig(String path) {
    return new GatewayConfig(effective.getConfig(path));
  }

  /** A few well-known keys. */
  public static final String FLIGHT_PORT = "fg.flight.port";
  public static final String FLIGHT_TLS_ENABLED = "fg.flight.tls.enabled";
  public static final String MEMORY_MAX = "fg.memory.max";
}
