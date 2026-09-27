package org.fg.frontend;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.fg.common.config.GatewayConfig;
import org.fg.orchestrator.store.OperationRow;
import org.fg.result.manifest.ResultManifest;
import org.fg.result.store.ObjectStoreService;
import org.fg.result.ticket.RelayTicket;
import org.fg.result.ticket.RelayTicketCodec;
import org.fg.result.ticket.TicketKind;

/**
 * endpoint/ticket 构造的唯一归属（design F3/D15/H4/v0.16）——所有取数票都在这里成型，续期
 * （RenewFlightEndpoint）与首发共用同一套构造器。网关对外只有两种形态，按行 mode 二选一、
 * 不可混发：
 *
 * <ul>
 *   <li><b>HTTPS（presign 形态）</b>：取数不经网关。每个物化 part 一个 endpoint——ticket 恒空
 *       （Flight v25 规范：非 gRPC location 的 endpoint 票置空，客户端见"空票 + http(s)
 *       location"即直接 HTTP GET）、location=presigned GET URL、expiration=签发时刻+TTL。
 *       续期 = 按 location 反解对象 key 后重签（{@link #renewedPresignedEndpoint}）。</li>
 *   <li><b>RELAY（中继形态）</b>：取数经网关 DoGet 中继。location 恒 reuseConnection（复用当前
 *       gRPC 连接），ticket 为 HMAC 信封票（{@link RelayTicket}），票形两种——STREAM：查询级
 *       单票单 endpoint 全量流（顺序敏感查询 H4 / single-stream 保险开关，交付保序）；
 *       PART：每 part 一票一 endpoint，可乱序消费、单分片可重试。续期 = 同字段重铸新
 *       issuedAt（{@link #renewedRelayEndpoint}），kind/partIndex 原样保留。</li>
 * </ul>
 */
final class EndpointsAssembler {

  /** Flight v25：非 gRPC location 的 endpoint 票置空（客户端据 location 直接 HTTP GET）。 */
  private static final Ticket EMPTY_TICKET = new Ticket(new byte[0]);

  private final GatewayConfig config;
  private final ObjectStoreService objects;
  private final RelayTicketCodec ticketCodec;
  private final String bucket;

  EndpointsAssembler(
      GatewayConfig config, ObjectStoreService objects, RelayTicketCodec ticketCodec) {
    this.config = config;
    this.objects = objects;
    this.ticketCodec = ticketCodec;
    this.bucket = config.getString("fg.result.bucket");
  }

  // ------------------------------------------------------------- 终态 / 快返装配

  /**
   * 终态交付的完整 endpoint 集（PollFlightInfo 终态一次性发出，flight_descriptor unset）。
   *
   * <p>HTTPS：每 part 一个 presigned endpoint。RELAY：行 ordered（或
   * {@code fg.result.relay.single-stream} 保险开关）→ 单个 STREAM endpoint；否则每 part 一个
   * PART endpoint（无序集合，客户端可乱序消费）。
   *
   * <p>D27 scroll：恒单 STREAM endpoint（注册时 mode 已强制 RELAY；此处置于 HTTPS 分支前
   * 作防御——页经 DoGet + x-fg-page-offset/limit 头切片，无头 DoGet = 全量顺序流）。
   * PART 扇出是"静态切分、顺序消费"模型，与随机访问语义不匹配。
   */
  List<FlightEndpoint> endpoints(OperationRow row, ResultManifest manifest) {
    if (row.kind() == OperationRow.Kind.COMMAND) {
      // D18：命令结果内联行内（无 manifest/part），恒单 COMMAND 定位票 endpoint
      return List.of(commandEndpoint(row));
    }
    if (row.scrollable()) {
      return List.of(streamEndpoint(row));
    }
    if (row.mode() == OperationRow.Mode.HTTPS) {
      List<FlightEndpoint> out = new ArrayList<>();
      for (ResultManifest.Part part : manifest.parts()) {
        out.add(presignedEndpoint(ObjectStoreService.objectName(part.uri())));
      }
      return out;
    }
    boolean singleStream = row.ordered() || config.getBoolean("fg.result.relay.single-stream");
    if (singleStream) {
      return List.of(streamEndpoint(row));
    }
    List<FlightEndpoint> out = new ArrayList<>();
    for (ResultManifest.Part part : manifest.parts()) {
      out.add(partEndpoint(row, part.index()));
    }
    return out;
  }

  /**
   * 快返 STREAM endpoint（H1：GetFlightInfo / PollFlightInfo 首响应即签发）。查询仍在途即返回，
   * schema 可能为空、无 records/bytes——客户端语义是"先拿票，DoGet 时再等"（relay 挂等待）。
   */
  FlightEndpoint streamEndpoint(OperationRow row) {
    return relayEndpoint(TicketKind.STREAM, row, null);
  }

  /**
   * COMMAND 定位 endpoint（D18）：非 SELECT 语句快返/终态共用——票只定位 queryId，
   * DoGet 按行内 command_result 内联交付（与 mode 无关：命令不经对象存储，无 presign 形态）。
   */
  FlightEndpoint commandEndpoint(OperationRow row) {
    return relayEndpoint(TicketKind.COMMAND, row, null);
  }

  /** RELAY PART endpoint：单分片票，分片 index 绑定票内（DoGet 据此定位单个 part 对象）。 */
  private FlightEndpoint partEndpoint(OperationRow row, int partIndex) {
    return relayEndpoint(TicketKind.PART, row, partIndex);
  }

  // ------------------------------------------------------------- 续期（RenewFlightEndpoint）

  /**
   * presign 形态续期：从过期 endpoint 的 location 反解对象 key，重签 presigned URL——新
   * endpoint、新 expiration（TTL 重新起算），底层对象不动。
   *
   * @throws IllegalArgumentException endpoint 不携带 location（非 presign 形态误入此分支）
   */
  FlightEndpoint renewedPresignedEndpoint(FlightEndpoint expired) {
    Location location = expired.getLocations().isEmpty() ? null : expired.getLocations().get(0);
    if (location == null) {
      throw new IllegalArgumentException("presigned endpoint carries no location");
    }
    return presignedEndpoint(objectKeyFromPresignedUrl(location.getUri()));
  }

  /**
   * relay 形态续期：验签解码旧票（HMAC + TTL + user 校验，见
   * {@link RelayTicketCodec#decode}）→ 同字段重铸，仅 issuedAt 换新——STREAM 续期仍是
   * STREAM，PART 续期仍是同号分片。
   *
   * @throws org.fg.result.ticket.InvalidTicketException 验签失败/票已过 TTL/user 不匹配
   */
  FlightEndpoint renewedRelayEndpoint(FlightEndpoint expired, String user) {
    String encoded = new String(expired.getTicket().getBytes(), StandardCharsets.UTF_8);
    RelayTicket fresh = ticketCodec.decode(encoded, user).withIssuedAt(epochNowSec());
    return new FlightEndpoint(relayTicket(fresh), Location.reuseConnection());
  }

  // ------------------------------------------------------------- 场景构造原语

  /** HTTPS presigned endpoint：空票 + presigned URL + expiration（签发时刻 + presign TTL）。 */
  private FlightEndpoint presignedEndpoint(String objectKey) {
    int ttl = presignTtlSeconds();
    String url = objects.presignGet(objectKey, ttl);
    return new FlightEndpoint(
        EMPTY_TICKET, Instant.now().plusSeconds(ttl), new Location(URI.create(url)));
  }

  /**
   * RELAY endpoint 统一构造：HMAC 信封票 + reuseConnection location。票负载字段与编码见
   * {@link RelayTicket}/{@link RelayTicketCodec}（bucket/prefix/queryId/user 取自行，partIndex
   * 仅 PART 必填）。
   */
  private FlightEndpoint relayEndpoint(TicketKind kind, OperationRow row, Integer partIndex) {
    RelayTicket ticket =
        new RelayTicket(
            kind, bucket, row.resultKeyPrefix(), row.queryId(), row.user(),
            epochNowSec(), partIndex);
    return new FlightEndpoint(relayTicket(ticket), Location.reuseConnection());
  }

  /** 编码信封票为 wire 形态（Ticket bytes = codec 输出的 UTF-8）。 */
  private Ticket relayTicket(RelayTicket ticket) {
    return new Ticket(ticketCodec.encode(ticket).getBytes(StandardCharsets.UTF_8));
  }

  // ------------------------------------------------------------- 小工具

  /** presigned URL 路径形如 {@code /{bucket}/{key}?sig...}——剥掉 bucket 段取对象 key。 */
  private static String objectKeyFromPresignedUrl(URI presignedUrl) {
    String path = presignedUrl.getPath();
    int secondSlash = path.indexOf('/', 1);
    return secondSlash > 0 ? path.substring(secondSlash + 1) : path.substring(1);
  }

  private int presignTtlSeconds() {
    return (int) (config.getDurationMs("fg.result.presign.ttl") / 1000);
  }

  private static long epochNowSec() {
    return System.currentTimeMillis() / 1000;
  }
}
