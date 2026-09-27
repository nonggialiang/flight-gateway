package org.fg.ha;

/**
 * Engine space path construction (D22, Kyuubi EngineRef 范式)——纯函数、两侧共享的路径契约。
 *
 * <pre>
 * engineRoot(ns, SHARE, spark)   = /{ns}_v1_{SHARE}_spark
 * engineSpace(root, user, sub)   = /{ns}_v1_{SHARE}_spark/{routingUser}/{subdomain}
 * connectionSpace(space, refId)  = {space}/{refId}            (CONNECTION：refId = fg 会话 id)
 * lockPath(root, user, sub)      = /{ns}_v1_{SHARE}_spark_lock/{routingUser}/{subdomain}
 * </pre>
 *
 * <p>lock 路径与 engineRoot 同级（root 加 {@code _lock} 后缀，Kyuubi EngineRef 同构），用于
 * 冷启动三段式的互斥（CONNECTION 免锁——space 内嵌唯一 refId，无需跨进程去重）。
 */
public final class EngineSpaces {

  /** 契约版本段：znode/路径格式不兼容演进时递增。 */
  public static final String CONTRACT_VERSION = "v1";

  private EngineSpaces() {}

  /** /{ns}_v1_{shareLevel}_{engineType} */
  public static String engineRoot(String namespace, String shareLevel, String engineType) {
    return "/" + requireNonBlank(namespace, "namespace") + "_" + CONTRACT_VERSION + "_"
        + requireNonBlank(shareLevel, "shareLevel").toUpperCase()
        + "_" + requireNonBlank(engineType, "engineType");
  }

  /** {engineRoot}/{routingUser}/{subdomain}（subdomain 空白归一为 default）。 */
  public static String engineSpace(String engineRoot, String routingUser, String subdomain) {
    return requireNonBlank(engineRoot, "engineRoot") + "/" + requireNonBlank(routingUser, "routingUser")
        + "/" + normalizedSubdomain(subdomain);
  }

  /** CONNECTION share level：engineSpace 追加唯一 refId（fg 会话 id，UUID 永不复用）。 */
  public static String connectionSpace(String engineSpace, String refId) {
    return requireNonBlank(engineSpace, "engineSpace") + "/" + requireNonBlank(refId, "refId");
  }

  /** {engineRoot}_lock/{routingUser}/{subdomain}（engineRoot 同级锁节点）。 */
  public static String lockPath(String engineRoot, String routingUser, String subdomain) {
    return requireNonBlank(engineRoot, "engineRoot") + "_lock/" + requireNonBlank(routingUser, "routingUser")
        + "/" + normalizedSubdomain(subdomain);
  }

  private static String normalizedSubdomain(String subdomain) {
    if (subdomain == null || subdomain.isBlank()) {
      return "default";
    }
    return subdomain.trim();
  }

  private static String requireNonBlank(String v, String name) {
    if (v == null || v.isBlank()) {
      throw new IllegalArgumentException(name + " required");
    }
    return v.trim();
  }
}
