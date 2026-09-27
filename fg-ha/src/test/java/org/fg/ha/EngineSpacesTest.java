package org.fg.ha;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class EngineSpacesTest {

  @Test
  void engineRootComposesNamespaceVersionShareEngine() {
    assertThat(EngineSpaces.engineRoot("flight-gateway", "USER", "spark"))
        .isEqualTo("/flight-gateway_v1_USER_spark");
    assertThat(EngineSpaces.engineRoot("ns", "CONNECTION", "spark"))
        .isEqualTo("/ns_v1_CONNECTION_spark");
  }

  @Test
  void engineSpaceAppendsUserAndSubdomain() {
    String root = EngineSpaces.engineRoot("ns", "USER", "spark");
    assertThat(EngineSpaces.engineSpace(root, "alice", "default"))
        .isEqualTo("/ns_v1_USER_spark/alice/default");
    assertThat(EngineSpaces.engineSpace(root, "alice", "team-a"))
        .isEqualTo("/ns_v1_USER_spark/alice/team-a");
  }

  @Test
  void blankSubdomainNormalizesToDefault() {
    String root = EngineSpaces.engineRoot("ns", "SERVER", "spark");
    assertThat(EngineSpaces.engineSpace(root, "shared", ""))
        .isEqualTo("/ns_v1_SERVER_spark/shared/default");
    assertThat(EngineSpaces.engineSpace(root, "shared", null))
        .isEqualTo("/ns_v1_SERVER_spark/shared/default");
  }

  @Test
  void connectionSpaceAppendsRefId() {
    String space = EngineSpaces.engineSpace(EngineSpaces.engineRoot("ns", "CONNECTION", "spark"), "alice", "default");
    assertThat(EngineSpaces.connectionSpace(space, "ref-123"))
        .isEqualTo("/ns_v1_CONNECTION_spark/alice/default/ref-123");
  }

  @Test
  void lockPathIsSiblingOfEngineRoot() {
    String root = EngineSpaces.engineRoot("ns", "USER", "spark");
    assertThat(EngineSpaces.lockPath(root, "alice", "default"))
        .isEqualTo("/ns_v1_USER_spark_lock/alice/default");
  }

  @Test
  void blankInputsRejected() {
    assertThatThrownBy(() -> EngineSpaces.engineRoot("", "USER", "spark"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> EngineSpaces.engineSpace("/r", " ", "d"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> EngineSpaces.connectionSpace("/r/u/d", ""))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
