package org.fg.spark.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.Map;

/**
 * MaterializationSpec 在引擎侧的解析（"spec" option JSON）。凭证不经 spec（D5：引擎进程静态
 * 配置）。
 *
 * <p>compression（D33）：none | zstd | lz4——IPC body 压缩编码（Arrow BodyCompression，
 * 读端透明解压）。空/未知值按 none 处理并告警（不因配置笔误断写）。
 */
public final class SpecOptions implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String objectUri;
  private final String compression;
  private final int maxRecordsPerBatch;

  public SpecOptions(String objectUri, String compression, int maxRecordsPerBatch) {
    this.objectUri = objectUri;
    this.compression = normalize(compression);
    this.maxRecordsPerBatch = maxRecordsPerBatch;
  }

  public static SpecOptions parse(Map<String, String> options, String fallbackPath) {
    String spec = options.get(FgResultDataSource.SPEC_OPTION);
    if (spec != null) {
      try {
        JsonNode node = MAPPER.readTree(spec);
        return new SpecOptions(
            node.path("objectUri").asText(fallbackPath),
            node.path("compression").asText("none"),
            node.path("maxRecordsPerBatchHint").asInt(16384));
      } catch (Exception e) {
        throw new IllegalArgumentException("Malformed spec option: " + spec, e);
      }
    }
    return new SpecOptions(fallbackPath, "none", 16384);
  }

  private static String normalize(String v) {
    String c = v == null ? "" : v.trim().toLowerCase();
    return c.equals("zstd") || c.equals("lz4") ? c : "none";
  }

  public String objectUri() {
    return objectUri;
  }

  public String compression() {
    return compression;
  }

  public int maxRecordsPerBatch() {
    return maxRecordsPerBatch;
  }
}
