package org.fg.result.manifest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * manifest.json 模型（design §4.4.2/§4.6）——结果真相源。引擎 commit 时写入；确定性路径
 * {@code {resultKeyPrefix}/manifest.json}。
 *
 * <p>part 顺序 = partition index（H4：结果顺序 = manifest 的 partition index 序 + part 内行序）。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ResultManifest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String queryId;
  private String schemaBase64;
  private List<Part> parts = new ArrayList<>();
  private long rowCount;
  private long bytes;
  private long createdAtEpochMs;

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class Part {
    @JsonProperty("uri")
    private String uri;

    @JsonProperty("index")
    private int index;

    @JsonProperty("recordCount")
    private long recordCount;

    @JsonProperty("bytes")
    private long bytes;

    @JsonProperty("attempt")
    private int attempt;

    public Part() {}

    public Part(String uri, int index, long recordCount, long bytes, int attempt) {
      this.uri = uri;
      this.index = index;
      this.recordCount = recordCount;
      this.bytes = bytes;
      this.attempt = attempt;
    }

    public String uri() {
      return uri;
    }

    public int index() {
      return index;
    }

    public long recordCount() {
      return recordCount;
    }

    public long bytes() {
      return bytes;
    }

    public int attempt() {
      return attempt;
    }
  }

  public static ResultManifest parse(byte[] json) {
    try {
      ResultManifest m = MAPPER.readValue(json, ResultManifest.class);
      m.parts.sort(Comparator.comparingInt(Part::index));
      return m;
    } catch (IOException e) {
      throw new IllegalStateException("Malformed manifest", e);
    }
  }

  public static ResultManifest readAllBytesThenParse(java.io.InputStream in) throws IOException {
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    in.transferTo(out);
    return parse(out.toByteArray());
  }

  public byte[] toJson() {
    try {
      return MAPPER.writeValueAsBytes(this);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to serialize manifest", e);
    }
  }

  public String queryId() {
    return queryId;
  }

  @JsonProperty("queryId")
  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public String schemaBase64() {
    return schemaBase64;
  }

  @JsonProperty("schemaBase64")
  public void setSchemaBase64(String schemaBase64) {
    this.schemaBase64 = schemaBase64;
  }

  public List<Part> parts() {
    return parts;
  }

  @JsonProperty("parts")
  public void setParts(List<Part> parts) {
    this.parts = parts;
  }

  public long rowCount() {
    return rowCount;
  }

  @JsonProperty("rowCount")
  public void setRowCount(long rowCount) {
    this.rowCount = rowCount;
  }

  public long bytes() {
    return bytes;
  }

  @JsonProperty("bytes")
  public void setBytes(long bytes) {
    this.bytes = bytes;
  }

  public long createdAtEpochMs() {
    return createdAtEpochMs;
  }

  @JsonProperty("createdAtEpochMs")
  public void setCreatedAtEpochMs(long createdAtEpochMs) {
    this.createdAtEpochMs = createdAtEpochMs;
  }
}
