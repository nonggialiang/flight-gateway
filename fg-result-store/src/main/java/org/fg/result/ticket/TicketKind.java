package org.fg.result.ticket;

/**
 * relay ticket 双 kind（design H5/D13，v0.16）：
 *
 * <ul>
 *   <li>STREAM：查询级——DoGet 按 manifest 顺序流全部 part；GetFlightInfo 快返路径唯一选项
 *       （完成前 part 数未知）；
 *   <li>PART：分片级——携 partIndex，DoGet 只流该 part；最终 poll 按 manifest 铸 N 张票，
 *       单分片可重试。
 * </ul>
 */
public enum TicketKind {
  STREAM,
  PART
}
