package org.fg.spi;

/** 背靠引擎 app 的会话；gateway 会话 ↔ 引擎会话一一映射（design F1）。 */
public interface EngineSession extends AutoCloseable {
  String sessionId();

  @Override
  void close() throws Exception;
}
