package org.fg.ha;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class EngineNodeTest {

  @Test
  void prefixAndParseRoundTrip() {
    String prefix = EngineNode.znodePrefix("hostA", 16001, 17001, "v9", "ref-1");
    assertThat(prefix).isEqualTo("serverUri=hostA:16001;adminPort=17001;version=v9;refId=ref-1;sequence=");

    // ZK appends the sequence digits to the prefix
    EngineNode parsed = EngineNode.parse(prefix + "0000000042",
        "hostA:16001".getBytes(StandardCharsets.UTF_8));
    assertThat(parsed.host()).isEqualTo("hostA");
    assertThat(parsed.connectPort()).isEqualTo(16001);
    assertThat(parsed.adminPort()).isEqualTo(17001);
    assertThat(parsed.version()).isEqualTo("v9");
    assertThat(parsed.refId()).isEqualTo("ref-1");
    assertThat(parsed.sequence()).isEqualTo(42L);
    assertThat(parsed.znodeName()).isEqualTo(prefix + "0000000042");
  }

  @Test
  void parseFallsBackToDataForServerUri() {
    EngineNode parsed = EngineNode.parse("adminPort=17002;version=v;refId=r;sequence=0000000007",
        "hostB:16002".getBytes(StandardCharsets.UTF_8));
    assertThat(parsed.host()).isEqualTo("hostB");
    assertThat(parsed.connectPort()).isEqualTo(16002);
    assertThat(parsed.adminPort()).isEqualTo(17002);
  }

  @Test
  void parseToleratesUnknownAttributesAndGaps() {
    EngineNode parsed = EngineNode.parse(
        "serverUri=hostC:16003;extra=x;sequence=notanumber", null);
    assertThat(parsed.host()).isEqualTo("hostC");
    assertThat(parsed.connectPort()).isEqualTo(16003);
    assertThat(parsed.adminPort()).isEqualTo(-1);
    assertThat(parsed.sequence()).isEqualTo(-1L);
    assertThat(parsed.refId()).isEmpty();
  }

  @Test
  void unparseableNodeRejected() {
    assertThatThrownBy(() -> EngineNode.parse("", null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> EngineNode.parse("sequence=1", null)) // no serverUri, no data
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void instanceIsHostPort() {
    assertThat(EngineNode.instance("h", 1)).isEqualTo("h:1");
  }
}
