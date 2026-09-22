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
 * 最终 FlightInfo endpoints 构造（design F3/D15/H4/v0.16）：mode 二选一不可混发——
 *
 * <ul>
 *   <li>HTTPS：N × {location=presigned URL, ticket 空, expiration_time=TTL}；</li>
 *   <li>RELAY：无序 N × PART 票（单分片可重试）；顺序敏感或 single-stream 保险开关 → 1 × STREAM 票。</li>
 * </ul>
 */
final class EndpointsAssembler {

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

  List<FlightEndpoint> endpoints(OperationRow row, ResultManifest manifest) {
    long issuedAt = System.currentTimeMillis() / 1000;
    List<FlightEndpoint> out = new ArrayList<>();
    if (row.mode() == OperationRow.Mode.HTTPS) {
      int ttl = (int) (config.getDurationMs("fg.result.presign.ttl") / 1000);
      for (ResultManifest.Part part : manifest.parts()) {
        String url = objects.presignGet(ObjectStoreService.objectName(part.uri()), ttl);
        out.add(
            new FlightEndpoint(
                new Ticket(new byte[0]),
                Instant.now().plusSeconds(ttl),
                new Location(URI.create(url))));
      }
      return out;
    }
    // RELAY
    boolean singleStream =
        row.ordered() || config.getBoolean("fg.result.relay.single-stream");
    if (singleStream) {
      RelayTicket ticket =
          new RelayTicket(
              TicketKind.STREAM, bucket, row.resultKeyPrefix(), row.queryId(), row.user(),
              issuedAt, null);
      out.add(
          new FlightEndpoint(
              new Ticket(ticketCodec.encode(ticket).getBytes(StandardCharsets.UTF_8)),
              Location.reuseConnection()));
      return out;
    }
    for (ResultManifest.Part part : manifest.parts()) {
      RelayTicket ticket =
          new RelayTicket(
              TicketKind.PART, bucket, row.resultKeyPrefix(), row.queryId(), row.user(),
              issuedAt, part.index());
      out.add(
          new FlightEndpoint(
              new Ticket(ticketCodec.encode(ticket).getBytes(StandardCharsets.UTF_8)),
              Location.reuseConnection()));
    }
    return out;
  }

  /** 快返路径的 STREAM 票 endpoint（H1：GetFlightInfo/PollFlightInfo 首响应即签发）。 */
  FlightEndpoint streamTicketEndpoint(OperationRow row) {
    RelayTicket ticket =
        new RelayTicket(
            TicketKind.STREAM,
            bucket,
            row.resultKeyPrefix(),
            row.queryId(),
            row.user(),
            System.currentTimeMillis() / 1000,
            null);
    return new FlightEndpoint(
        new Ticket(ticketCodec.encode(ticket).getBytes(StandardCharsets.UTF_8)),
        Location.reuseConnection());
  }

  RelayTicketCodec codec() {
    return ticketCodec;
  }
}
