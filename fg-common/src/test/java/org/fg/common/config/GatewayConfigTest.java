package org.fg.common.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 用户配置层装载（-Dfg.config.file 显式路径 / classpath gateway.conf / 三层合并序）。 */
class GatewayConfigTest {

  @AfterEach
  void tearDown() {
    System.clearProperty(GatewayConfig.CONFIG_FILE_PROPERTY);
    System.clearProperty("fg.test.key");
    System.clearProperty("fg.flight.port");
  }

  @Test
  void explicitFileOverridesReferenceDefaults(@TempDir Path tmp) throws Exception {
    Path conf = tmp.resolve("gateway.conf");
    Files.writeString(conf, "fg.flight.port = 32099\nfg.test { key = from-file }\n");
    System.setProperty(GatewayConfig.CONFIG_FILE_PROPERTY, conf.toString());

    GatewayConfig config = GatewayConfig.create();
    assertThat(config.getInt("fg.flight.port")).isEqualTo(32099);
    assertThat(config.getString("fg.test.key")).isEqualTo("from-file");
    // 未覆盖键回落 reference 默认
    assertThat(config.getInt("fg.engine.spark.admin.port")).isEqualTo(15003);
  }

  @Test
  void systemPropertyBeatsExplicitFile(@TempDir Path tmp) throws Exception {
    Path conf = tmp.resolve("gateway.conf");
    Files.writeString(conf, "fg.test.key = from-file\n");
    System.setProperty(GatewayConfig.CONFIG_FILE_PROPERTY, conf.toString());
    System.setProperty("fg.test.key", "from-d");

    assertThat(GatewayConfig.create().getString("fg.test.key")).isEqualTo("from-d");
  }

  @Test
  void missingExplicitFileFailsFast() {
    System.setProperty(GatewayConfig.CONFIG_FILE_PROPERTY, "/no/such/gateway.conf");
    assertThatThrownBy(GatewayConfig::create)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("fg.config.file")
        .hasMessageContaining("missing file");
  }

  @Test
  void noExplicitFileFallsBackToClasspath() {
    // 无 classpath gateway.conf（测试 classpath 未放置）→ reference 默认直接生效
    GatewayConfig config = GatewayConfig.create();
    assertThat(config.getInt("fg.flight.port")).isEqualTo(32010);
    assertThat(config.getBoolean("fg.metrics.enabled")).isTrue();
  }
}
