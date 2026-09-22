package org.fg.result.ticket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/** Ticket HMAC 信封：编解码/验签/过期/篡改/user 校验/双密钥轮换（design D13）。 */
class TestRelayTicketCodec {

  private static final long NOW = System.currentTimeMillis() / 1000;

  private final RelayTicketCodec codec =
      new RelayTicketCodec("current-secret", "previous-secret", Duration.ofHours(1));

  @Test
  void streamTicketRoundTrip() {
    RelayTicket ticket =
        new RelayTicket(TicketKind.STREAM, "bkt", "spark/alice/q1", "q1", "alice", NOW, null);
    String encoded = codec.encode(ticket);
    RelayTicket decoded = codec.decode(encoded, "alice");
    assertThat(decoded).isEqualTo(ticket);
    assertThat(decoded.manifestObjectKey()).isEqualTo("spark/alice/q1/manifest.json");
  }

  @Test
  void partTicketRoundTrip() {
    RelayTicket ticket =
        new RelayTicket(TicketKind.PART, "bkt", "spark/alice/q1", "q1", "alice", NOW, 3);
    assertThat(codec.decode(codec.encode(ticket), "alice").partIndex()).isEqualTo(3);
  }

  @Test
  void tamperedSignatureRejected() {
    RelayTicket ticket =
        new RelayTicket(TicketKind.STREAM, "bkt", "pfx", "q1", "alice", NOW, null);
    String encoded = codec.encode(ticket);
    String tampered = encoded.substring(0, encoded.length() - 1);
    char flip = encoded.charAt(encoded.length() - 1) == 'a' ? 'b' : 'a';
    tampered += flip;
    String finalTampered = tampered;
    assertThatThrownBy(() -> codec.decode(finalTampered, "alice"))
        .isInstanceOf(RelayTicketCodec.InvalidTicketException.class)
        .hasMessageContaining("Bad signature");
  }

  @Test
  void userMismatchRejected() {
    RelayTicket ticket =
        new RelayTicket(TicketKind.STREAM, "bkt", "pfx", "q1", "alice", NOW, null);
    String encoded = codec.encode(ticket);
    assertThatThrownBy(() -> codec.decode(encoded, "bob"))
        .isInstanceOf(RelayTicketCodec.InvalidTicketException.class)
        .hasMessageContaining("user mismatch");
  }

  @Test
  void expiredRejected() {
    RelayTicketCodec shortTtl = new RelayTicketCodec("k", null, Duration.ofSeconds(1));
    long now = System.currentTimeMillis() / 1000;
    RelayTicket fresh =
        new RelayTicket(TicketKind.STREAM, "bkt", "pfx", "q1", "alice", now, null);
    assertThat(shortTtl.decode(shortTtl.encode(fresh), "alice")).isEqualTo(fresh);

    RelayTicket stale =
        new RelayTicket(TicketKind.STREAM, "bkt", "pfx", "q1", "alice", now - 10, null);
    assertThatThrownBy(() -> shortTtl.decode(shortTtl.encode(stale), "alice"))
        .isInstanceOf(RelayTicketCodec.ExpiredTicketException.class);
  }

  @Test
  void previousKeyStillValidatesDuringRotation() {
    RelayTicketCodec oldSigner = new RelayTicketCodec("previous-secret", null, Duration.ofHours(1));
    RelayTicketCodec rotator =
        new RelayTicketCodec("current-secret", "previous-secret", Duration.ofHours(1));
    RelayTicket ticket =
        new RelayTicket(TicketKind.PART, "bkt", "pfx", "q1", "alice", NOW, 0);
    String signedByOld = oldSigner.encode(ticket);
    assertThat(rotator.decode(signedByOld, "alice")).isEqualTo(ticket);
  }

  @Test
  void partTicketRequiresIndex() {
    assertThatThrownBy(() -> new RelayTicket(TicketKind.PART, "bkt", "pfx", "q1", "a", 1L, null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new RelayTicket(TicketKind.STREAM, "bkt", "pfx", "q1", "a", 1L, 2))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
