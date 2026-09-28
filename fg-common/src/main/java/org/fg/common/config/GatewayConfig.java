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
import java.util.Map;

/** Gateway configuration, DremioConfig-style three-layer merge. */
public final class GatewayConfig {

  public static final String REFERENCE_CONF = "fg-reference.conf";
  public static final String CONFIG_FILE = "gateway.conf";
  /** 显式指定 gateway.conf 路径的系统属性（-jar 模式无需 -cp 挂目录）。 */
  public static final String CONFIG_FILE_PROPERTY = "fg.config.file";

  private final Config effective;

  private GatewayConfig(Config effective) {
    this.effective = effective;
  }

  /**
   * 用户配置层装载：{@code -Dfg.config.file} 指定路径则 parseFile（文件必须存在，
   * 缺失即抛——显式指定路径拼错不应静默回落）；否则回落 classpath 的 gateway.conf
   * （可缺席）。reference ← user ← -Dfg.* 三层合并不变。
   */
  public static GatewayConfig create() {
    final Config reference = ConfigFactory.parseResources(REFERENCE_CONF);
    final String explicitPath = System.getProperty(CONFIG_FILE_PROPERTY, "").trim();
    final Config user;
    if (!explicitPath.isEmpty()) {
      java.io.File file = new java.io.File(explicitPath);
      if (!file.isFile()) {
        throw new IllegalStateException(
            CONFIG_FILE_PROPERTY + " points to a missing file: " + explicitPath);
      }
      user = ConfigFactory.parseFile(file);
    } else {
      user = ConfigFactory.parseResources(CONFIG_FILE);
    }
    return new GatewayConfig(applySystemProperties(user.withFallback(reference)).resolve());
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

  /** 字符串列表（D22：group-mapping / auth users / extra-jars 等）。 */
  public List<String> getStringList(String path) {
    return effective.getStringList(path);
  }

  /**
   * 配置对象的扁平键值展开（D22：{@code fg.engine.spark.launch.conf.*} 透传用）。
   * HOCON 点号嵌套还原为点号键：{@code launch.conf.spark.driver.memory} →
   * {@code spark.driver.memory}。叶子非字符串值以 toString 交付。
   */
  public Map<String, String> getFlatEntries(String path) {
    if (!effective.hasPath(path)) {
      return Map.of();
    }
    Map<String, String> out = new java.util.LinkedHashMap<>();
    flatten(effective.getObject(path).unwrapped(), "", out);
    return out;
  }

  @SuppressWarnings("unchecked")
  private static void flatten(Object node, String prefix, Map<String, String> out) {
    if (node instanceof Map) {
      for (Map.Entry<String, Object> e : ((Map<String, Object>) node).entrySet()) {
        flatten(e.getValue(), prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey(), out);
      }
    } else if (node != null) {
      out.put(prefix, String.valueOf(node));
    }
  }

  /** A few well-known keys. */
  public static final String FLIGHT_PORT = "fg.flight.port";
  public static final String FLIGHT_TLS_ENABLED = "fg.flight.tls.enabled";
  public static final String MEMORY_MAX = "fg.memory.max";
}
