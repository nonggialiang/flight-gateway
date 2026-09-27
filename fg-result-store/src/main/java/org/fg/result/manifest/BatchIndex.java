package org.fg.result.manifest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;

/**
 * per-part batch 索引（D27，{part}.bidx 边车）：每 batch 的 {@code (offset, length, rows)}
 * 三元组——encapsulated message 自包含，区间字节可经 Range GET 独立读取解码。
 *
 * <p>边车是<strong>优化而非正确性依赖</strong>：缺席/损坏时 {@link #parse} 返回的调用方应
 * 回落整 part 顺序读（{@code ObjectStoreService.readBatchIndex} → null）。
 */
public final class BatchIndex {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final List<Batch> batches;

  private BatchIndex(List<Batch> batches) {
    this.batches = batches;
  }

  /** 解析边车 JSON（{"batches":[[offset,length,rows],...]}）；非法输入抛 RuntimeException。 */
  public static BatchIndex parse(byte[] json) {
    try {
      JsonNode root = MAPPER.readTree(json);
      JsonNode arr = root.get("batches");
      if (arr == null || !arr.isArray()) {
        throw new IllegalArgumentException("missing 'batches' array");
      }
      List<Batch> out = new ArrayList<>(arr.size());
      for (JsonNode t : arr) {
        if (t.size() != 3) {
          throw new IllegalArgumentException("batch triple expected: " + t);
        }
        out.add(new Batch(t.get(0).asLong(), t.get(1).asLong(), t.get(2).asLong()));
      }
      return new BatchIndex(out);
    } catch (Exception e) {
      throw new IllegalArgumentException("unparseable batch index", e);
    }
  }

  public List<Batch> batches() {
    return batches;
  }

  /** 单 batch 三元组：part 文件内字节区间 [offset, offset+length) 恰为一个自包含 message。 */
  public record Batch(long offset, long length, long rows) {}
}
