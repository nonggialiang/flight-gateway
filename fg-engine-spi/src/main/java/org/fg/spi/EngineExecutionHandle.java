package org.fg.spi;

/**
 * 引擎侧执行句柄（opaque、可字符串化落库）。例如 Spark Connect 的 operationId。
 *
 * <p>语义：submit 首响应捕获；cancel/interrupt、attach（reattach）、release 均以此为对象。
 */
public record EngineExecutionHandle(String engineType, String handle) {

  public EngineExecutionHandle {
    if (engineType == null || handle == null || handle.isBlank()) {
      throw new IllegalArgumentException("engineType/handle must be non-null and handle non-blank");
    }
  }

  public static EngineExecutionHandle parse(String stored) {
    int idx = stored.indexOf(':');
    if (idx <= 0 || idx == stored.length() - 1) {
      throw new IllegalArgumentException("Malformed EngineExecutionHandle: " + stored);
    }
    return new EngineExecutionHandle(stored.substring(0, idx), stored.substring(idx + 1));
  }

  public String encode() {
    return engineType + ":" + handle;
  }

  @Override
  public String toString() {
    return encode();
  }
}
