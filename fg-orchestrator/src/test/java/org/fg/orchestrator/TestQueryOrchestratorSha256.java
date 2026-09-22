package org.fg.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TestQueryOrchestratorSha256 {

  @Test
  void sha256Stable() {
    assertThat(QueryOrchestrator.sha256("SELECT 1"))
        .isEqualTo(QueryOrchestrator.sha256("SELECT 1"))
        .hasSize(64);
    assertThat(QueryOrchestrator.sha256("SELECT 1"))
        .isNotEqualTo(QueryOrchestrator.sha256("SELECT 2"));
  }
}
