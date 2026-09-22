package org.fg.spi;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * gateway 侧引擎控制面契约（design §4.3）。
 *
 * <p>语义按 v0.13+ reattachable 主路径建模（设计 §2.2/§4.4.3）：
 *
 * <ul>
 *   <li>{@link #submit}：提交即 detach——触发执行，首响应捕获 {@link EngineExecutionHandle}
 *       后回调 {@link SubmitListener#onHandle} 并立即断流，触发实例自此对查询无状态；
 *   <li>{@link #attach}：经 DB attach 租约仲裁后的赢家挂执行流直至终态；attach 不承载正确性
 *       （持久事实是 manifest），流断返回 UNKNOWN 由对账兜底；
 *   <li>{@link #interrupt}：取消，不要求 attach（陪伴的 attach 者不受影响）；
 *   <li>{@link #releaseExecution}：attach 者终态后的释放义务。
 * </ul>
 *
 * <p>边界原则：gateway 只认识 sql + {@link MaterializationSpec}；"怎么执行、怎么写、元数据从哪来"
 * 全部在 Engine Kit 里。协议层/编排层只 import SPI，不 import 任何 Kit。
 */
public interface SqlEngine extends AutoCloseable {

  String type();

  EngineSession openSession(GatewaySession ctx) throws Exception;

  /** 有界 AnalyzePlan：返回 result schema（列名已去重）；超时抛异常。 */
  Schema analyzeSchema(EngineSession session, String sql, Duration timeout) throws Exception;

  /**
   * 检测 SQL 顶层是否全局排序（ORDER BY）——H4：顺序敏感查询写端/票形需单 endpoint。
   * 实现可用 AnalyzePlan 或轻量解析。
   */
  boolean isOrderSensitive(EngineSession session, String sql) throws Exception;

  /** 提交即 detach（见接口 javadoc）。返回的 future 在 detach 完成时完成（不代表执行完成）。 */
  CompletableFuture<EngineExecutionHandle> submit(
      EngineSession session, String sql, MaterializationSpec spec, SubmitListener listener);

  /** attach 挂执行流；返回终态快速结果。 */
  CompletableFuture<ExecutionOutcome> attach(EngineSession session, EngineExecutionHandle handle);

  /** Interrupt：取消执行；不要求 attach。引擎不可达抛异常（调用方转 UNSPECIFIED）。 */
  void interrupt(EngineSession session, EngineExecutionHandle handle) throws Exception;

  /** 释放引擎侧响应缓冲（attach 者义务）。 */
  void releaseExecution(EngineExecutionHandle handle);

  EngineCatalog catalog(EngineSession session);

  /** submit 过程回调（触发实例用）。 */
  interface SubmitListener {
    /** 首响应捕获引擎执行句柄（触发实例 UPDATE 行后 detach）。 */
    void onHandle(EngineExecutionHandle handle);
  }
}
