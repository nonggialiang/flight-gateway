package org.fg.spark.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.Map;

/**
 * MaterializationSpec 在引擎侧的解析（"spec" option JSON）。凭证不经 spec（D5：引擎进程静态
 * 配置）。
 */
public final class SpecOptions implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String objectUri;
  private final boolean zstdCompression;
  private final int maxRecordsPerBatch;

  public SpecOptions(String objectUri, boolean zstdCompression, int maxRecordsPerBatch) {
    this.objectUri = objectUri;
    this.zstdCompression = zstdCompression;
    this.maxRecordsPerBatch = maxRecordsPerBatch;
  }

  public static SpecOptions parse(Map<String, String> options, String fallbackPath) {
    String spec = options.get(FgResultDataSource.SPEC_OPTION);
    if (spec != null) {
      try {
        JsonNode node = MAPPER.readTree(spec);
        return new SpecOptions(
            node.path("objectUri").asText(fallbackPath),
            node.path("zstdCompression").asBoolean(false),
            node.path("maxRecordsPerBatchHint").asInt(16384));
      } catch (Exception e) {
        throw new IllegalArgumentException("Malformed spec option: " + spec, e);
      }
    }
    return new SpecOptions(fallbackPath, false, 16384);
  }

  public String objectUri() {
    return objectUri;
  }

  public boolean zstdCompression() {
    return zstdCompression;
  }

  public int maxRecordsPerBatch() {
    return maxRecordsPerBatch;
  }
}
