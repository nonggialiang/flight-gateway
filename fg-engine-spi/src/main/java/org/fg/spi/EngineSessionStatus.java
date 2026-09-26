package org.fg.spi;

/**
 * 引擎侧会话状态（D20 生命周期绑定）：alive=false 表示会话已不存在（被逐出/引擎重启）；
 * engineStartedAtMs 为引擎侧会话创建时刻——与网关登记的化身标记比对，同 id 不同生即
 * "转世"（引擎重启后同 id 被重建），视同已死。
 *
 * @param alive           会话是否仍在引擎侧存活
 * @param engineStartedAt 引擎侧会话创建 epoch millis（alive=false 时无意义，为 0）
 */
public record EngineSessionStatus(boolean alive, long engineStartedAt) {

  public static EngineSessionStatus dead() {
    return new EngineSessionStatus(false, 0L);
  }
}
