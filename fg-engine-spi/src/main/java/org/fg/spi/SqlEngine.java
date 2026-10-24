package org.fg.spi;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * gateway 侧引擎控制面契约（design §4.3）。
 *
 * <p>语义按 v0.13+ reattachable 主路径建模（设计 §2.2/§4.4.3）：
 *
 * <ul>
 *   <li>{@link #submit}：提交即 detach——携带编排层预生成的 {@link EngineExecutionHandle}
 *       （Spark Connect 3.5+ 支持客户端指定 operation_id，服务端按 UUID 规范化后作执行身份）
 *       随 executePlan 下发，首响应回显句柄后立即断流，触发实例自此对查询无状态；
 *   <li>{@link #attach}：挂执行流直至终态。D36 起无跨实例租约——各网关实例可各自 attach
 *       同一 operationId（实例内去重），attach 不承载正确性（持久事实是 manifest），流断
 *       返回 UNKNOWN 由对账兜底；
 *   <li>{@link #interrupt}：取消，不要求 attach（陪伴的 attach 者不受影响）；
 *   <li>{@link #releaseExecution}：attach 者终态后的释放义务（幂等）。
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

  /**
   * 提交即 detach（见接口 javadoc）。{@code clientHandle} 为编排层预生成的执行句柄——随
   * executePlan 的 operation_id 下发（引擎按其规范化后作执行身份），行上自 INSERT 起即可
   * attach/interrupt/release，消除"提交在途、operationId 未知"窗口。返回的 future 在 detach
   * 完成时完成（不代表执行完成），完成值即 {@code clientHandle}（首响应回显一致时）。
   */
  CompletableFuture<EngineExecutionHandle> submit(
      EngineSession session,
      String sql,
      MaterializationSpec spec,
      EngineExecutionHandle clientHandle,
      SubmitListener listener);

  /**
   * 命令执行（D17/D18，非 SELECT：SET/SHOW/DESCRIBE/EXPLAIN/USE/DDL/DML）：无物化、结果
   * 小、网关持流至终态。返回的 future 在命令终态时完成（COMPLETED 携带 schema+IPC 结果；
   * FAILED/CANCELLED 携带 error）。复用 {@link SubmitListener#onHandle} 捕获引擎
   * operationId（cancel 链同 submit：setOperationId + interrupt）。
   */
  CompletableFuture<CommandOutcome> executeCommand(
      EngineSession session, String sql, SubmitListener listener);

  /** attach 挂执行流；返回终态快速结果。 */
  CompletableFuture<ExecutionOutcome> attach(EngineSession session, EngineExecutionHandle handle);

  /**
   * D38 inline 提交：同 {@link #submit} 的提交即 detach，但 plan 为裸 SQL 关系（无
   * WriteOperation/无物化）+ ReattachOptions——结果经 {@link #attachInline} 增量消费。
   */
  CompletableFuture<EngineExecutionHandle> submitInline(
      EngineSession session, String sql, EngineExecutionHandle clientHandle, SubmitListener listener);

  /**
   * D38 inline 消费 attach：{@code ReattachExecute(last_response_id)}——{@code lastResponseId}
   * 为 null 从头消费。逐批回调 {@link InlineConsumer}（回调线程=attach 流的 onNext 线程，
   * 实现必须非阻塞或自行转交；观察者绝不抛）。返回 future 终态语义同 {@link #attach}。
   */
  CompletableFuture<ExecutionOutcome> attachInline(
      EngineSession session,
      EngineExecutionHandle handle,
      String lastResponseId,
      InlineConsumer consumer);

  /**
   * D38 前向单游标释放：earliest_unreleased 前推到 {@code responseId} 之后（该 id 之前的
   * 缓存响应永久不可再读——续传请求落在已释放区间由引擎报 INVALID_CURSOR，实现侧幂等）。
   */
  void releaseUntil(EngineExecutionHandle handle, String responseId);

  /**
   * D38：inline 逐批消费者。
   *
   * <p>{@code ipcStreamBytes} = Spark Connect ArrowBatch.data 的完整 Arrow IPC stream
   * bytes（pyspark 同款消费方式：{@code pa.ipc.open_stream(batch.data)}——自含 framing，
   * ArrowStreamReader 可直读）。responseId = 引擎响应流位置标识（客户端续传游标，经
   * app_metadata 回传）。
   */
  interface InlineConsumer {
    void onSchema(org.apache.arrow.vector.types.pojo.Schema schema);

    void onBatch(String responseId, byte[] ipcStreamBytes);

    void onResultComplete();
  }

  /** Interrupt：取消执行；不要求 attach。引擎不可达抛异常（调用方转 UNSPECIFIED）。 */
  void interrupt(EngineSession session, EngineExecutionHandle handle) throws Exception;

  /** 释放引擎侧响应缓冲（attach 者义务）。 */
  void releaseExecution(EngineExecutionHandle handle);

  EngineCatalog catalog(EngineSession session);

  // ------------------------------------------------------------- 会话生命周期（D20）

  /**
   * 会话状态/化身校验（D20 生命周期绑定）。实现<b>不得有副作用</b>：不得经普通 Connect
   * RPC 探测——Spark Connect 收到未知 session_id 的请求会静默重建同 id 会话，探测即污染。
   * 引擎 kit 应经自带 admin 通道读服务端会话登记。
   */
  EngineSessionStatus sessionStatus(EngineSession session) throws Exception;

  /**
   * 关闭引擎侧会话（客户端 CloseSession → 网关登记 CLOSED 前的引擎侧资源释放：在途执行
   * 中断 + 会话逐出）。Spark 3.5 协议面无此 RPC，由 kit 经 session-admin 实现；升级 4.x
   * 后换原生 ReleaseSession，本签名不动。
   */
  void closeSession(EngineSession session) throws Exception;

  /**
   * 会话选项（D20，不落盘——生命周期即引擎会话生命周期）：即时代理到引擎会话 conf。
   * toSet 批量设；toUnset 清除（Flight SQL SessionOptionValue 空值 = 清除语义）。
   */
  void setSessionConf(EngineSession session, Map<String, String> toSet, Set<String> toUnset)
      throws Exception;

  /** 会话选项回读（GetSessionOptions 用）：keys 为已登记选项键，值实时取引擎会话。 */
  Map<String, String> getSessionConf(EngineSession session, Collection<String> keys) throws Exception;

  /** submit 过程回调（触发实例用）。 */
  interface SubmitListener {
    /** 首响应捕获引擎执行句柄（触发实例 UPDATE 行后 detach）。 */
    void onHandle(EngineExecutionHandle handle);
  }
}
