package org.fg.result.ticket;

/**
 * relay ticket 三 kind（design H5/D13/D18，v0.16/v0.18）：
 *
 * <ul>
 *   <li>STREAM：查询级——DoGet 按 manifest 顺序流全部 part；GetFlightInfo 快返路径唯一选项
 *       （完成前 part 数未知）；</li>
 *   <li>PART：分片级——携 partIndex，DoGet 只流该 part；最终 poll 按 manifest 铸 N 张票，
 *       单分片可重试；</li>
 *   <li>COMMAND：命令级定位器（D18）——SET/SHOW/DESCRIBE/EXPLAIN/USE/DDL/DML 非 SELECT
 *       语句；结果内联 fg_operation.command_result（无 manifest、无 part），票只定位
 *       queryId、无 partIndex；DoGet 等待预算同 STREAM（挂 fg.query.timeout）。</li>
 * </ul>
 */
public enum TicketKind {
  STREAM,
  PART,
  COMMAND
}
