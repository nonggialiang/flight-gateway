package org.fg.spark.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * sink 本地集成（design §9：part IPC 格式正确性/manifest 顺序/分区数精确/空分区不产文件）。
 * 本地文件系统即对象存储替身（objectUri=file:///）。
 */
class TestFgResultSinkLocalFs {

  private static SparkSession spark;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir Path tmp;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("fg-sink-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private String queryDir(String queryId) {
    return tmp.toUri() + queryId + "/";
  }

  private void write(String dir, int parts, long rows) {
    Dataset<Row> df = spark.range(0, rows).toDF("id").selectExpr("id", "id as v");
    if (parts == 1) {
      df = df.coalesce(1);
    } else if (parts > 0) {
      df = df.repartition(parts);
    }
    df.write()
        .format("fg-result")
        .mode("overwrite")
        .option(
            "spec",
            "{\"objectUri\":\"" + dir + "\",\"compression\":\"none\",\"maxRecordsPerBatchHint\":256}")
        .save(dir);
  }

  @Test
  void multiPartWriteProducesManifestInIndexOrder() throws Exception {
    String dir = queryDir("q-multi");
    write(dir, 3, 1000);

    Path manifestPath = Path.of(java.net.URI.create(dir + "manifest.json"));
    assertThat(manifestPath).exists();
    JsonNode manifest = MAPPER.readTree(Files.readAllBytes(manifestPath));

    assertThat(manifest.get("rowCount").asLong()).isEqualTo(1000);
    List<Integer> indexes = new ArrayList<>();
    manifest.get("parts").forEach(p -> indexes.add(p.get("index").asInt()));
    assertThat(indexes).isSorted().hasSize(3);
    assertThat(manifest.get("schemaBase64").asText()).isNotBlank();

    // part 是可读的 Arrow IPC stream，行数总和正确
    long total = 0;
    for (JsonNode p : manifest.get("parts")) {
      try (InputStream in = Files.newInputStream(Path.of(java.net.URI.create(p.get("uri").asText())));
          ArrowStreamReader reader = new ArrowStreamReader(in, new org.apache.arrow.memory.RootAllocator(Integer.MAX_VALUE))) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        while (reader.loadNextBatch()) {
          total += root.getRowCount();
        }
      }
    }
    assertThat(total).isEqualTo(1000);
  }

  @Test
  void singlePartWriteProducesExactlyOnePart() throws Exception {
    String dir = queryDir("q-single");
    write(dir, 1, 500);

    JsonNode manifest =
        MAPPER.readTree(Files.readAllBytes(Path.of(java.net.URI.create(dir + "manifest.json"))));
    assertThat(manifest.get("parts")).hasSize(1);
    assertThat(manifest.get("parts").get(0).get("recordCount").asLong()).isEqualTo(500);
    // 唯一 part 必须整体是顺序 IPC stream（可从 0 读到尾）
    long rows = 0;
    try (InputStream in =
            Files.newInputStream(
                Path.of(java.net.URI.create(manifest.get("parts").get(0).get("uri").asText())));
        ArrowStreamReader reader =
            new ArrowStreamReader(in, new org.apache.arrow.memory.RootAllocator(Integer.MAX_VALUE))) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      while (reader.loadNextBatch()) {
        rows += root.getRowCount();
      }
    }
    assertThat(rows).isEqualTo(500);
  }

  @Test
  void emptyPartitionProducesNoFile() throws Exception {
    String dir = queryDir("q-empty");
    // range(0,0) 无行：所有分区空，不产 part，manifest 仍写入且 parts=[]
    write(dir, 0, 0);

    JsonNode manifest =
        MAPPER.readTree(Files.readAllBytes(Path.of(java.net.URI.create(dir + "manifest.json"))));
    assertThat(manifest.get("parts").size()).isZero();
    assertThat(manifest.get("rowCount").asLong()).isZero();
  }

  /**
   * D27 batch 索引边车 round-trip：{part}.bidx 的每个 (offset,length) 区间必须恰好是一个
   * 自包含 encapsulated message——按区间随机读出的行集合与顺序整读该 part 完全一致，
   * 且 rows 三元组之和 = manifest recordCount。这是网关 Range GET 分页的正确性前提。
   */
  @Test
  void batchIndexSidecarSupportsRandomMessageReads() throws Exception {
    String dir = queryDir("q-bidx");
    write(dir, 2, 1000); // maxRecordsPerBatchHint=256 → 每 part 多个 batch

    JsonNode manifest =
        MAPPER.readTree(Files.readAllBytes(Path.of(java.net.URI.create(dir + "manifest.json"))));
    assertThat(manifest.get("parts").size()).isEqualTo(2);

    try (org.apache.arrow.memory.RootAllocator allocator =
        new org.apache.arrow.memory.RootAllocator(Integer.MAX_VALUE)) {
      for (JsonNode p : manifest.get("parts")) {
        String partUri = p.get("uri").asText();
        Path bidxPath = Path.of(java.net.URI.create(partUri + FgResultSinks.BATCH_INDEX_SUFFIX));
        assertThat(bidxPath).exists();
        JsonNode bidx = MAPPER.readTree(Files.readAllBytes(bidxPath));
        assertThat(bidx.get("batches").size()).isGreaterThan(1);

        // 顺序整读收集行值与 batch 边界
        List<Long> sequential = new ArrayList<>();
        List<Integer> seqBatchRows = new ArrayList<>();
        try (InputStream in = Files.newInputStream(Path.of(java.net.URI.create(partUri)));
            ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          while (reader.loadNextBatch()) {
            seqBatchRows.add(root.getRowCount());
            for (int i = 0; i < root.getRowCount(); i++) {
              sequential.add(root.getVector("id").getObject(i) instanceof Number n ? n.longValue() : -1L);
            }
          }
        }

        // 三元组 rows 与实际 batch 行数一致，offset/length 区间互斥连续（相对首个 batch）
        long rowsSum = 0;
        long expectedOffset = -1;
        for (JsonNode t : bidx.get("batches")) {
          long offset = t.get(0).asLong();
          long length = t.get(1).asLong();
          long rows = t.get(2).asLong();
          if (expectedOffset < 0) {
            expectedOffset = offset; // 首 batch 起点不要求为 0（schema message 在前）
          }
          assertThat(length).isPositive();
          assertThat(offset).isEqualTo(expectedOffset);
          expectedOffset = offset + length;
          rowsSum += rows;
        }
        int batchIdx = 0;
        for (JsonNode t : bidx.get("batches")) {
          assertThat(t.get(2).asLong()).isEqualTo(seqBatchRows.get(batchIdx).longValue());
          batchIdx++;
        }
        assertThat(rowsSum).isEqualTo(p.get("recordCount").asLong());

        // 按区间随机读（网关 Range GET 同款路径）：
        // ① 结构校验——区间必须恰好是一个自包含 encapsulated message（continuation + metadata
        //    长度 + flatbuffer Message(RecordBatch) + 对齐 + body），总长精确等于 length；
        // ② 内容校验——schema message（part 头部字节）+ 区间 + EOS 拼成 mini stream 顺序读，
        //    行值与顺序整读一致（网关侧以 manifest.schemaBase64 替代 part 头部，同构）
        byte[] partBytes = Files.readAllBytes(Path.of(java.net.URI.create(partUri)));
        long firstBatchOffset = bidx.get("batches").get(0).get(0).asLong();
        byte[] schemaMessage =
            java.util.Arrays.copyOfRange(partBytes, 0, (int) firstBatchOffset);
        List<Long> random = new ArrayList<>();
        for (JsonNode t : bidx.get("batches")) {
          long offset = t.get(0).asLong();
          long length = t.get(1).asLong();
          int o = (int) offset;

          // ① encapsulated message 结构与总长（4B continuation=0xFFFFFFFF + 4B metadata len）
          assertThat(partBytes[o]).isEqualTo((byte) 0xFF);
          assertThat(partBytes[o + 1]).isEqualTo((byte) 0xFF);
          assertThat(partBytes[o + 2]).isEqualTo((byte) 0xFF);
          assertThat(partBytes[o + 3]).isEqualTo((byte) 0xFF);
          int metaLen = (partBytes[o + 4] & 0xFF) | (partBytes[o + 5] & 0xFF) << 8
              | (partBytes[o + 6] & 0xFF) << 16 | (partBytes[o + 7] & 0xFF) << 24;
          org.apache.arrow.flatbuf.Message msg =
              org.apache.arrow.flatbuf.Message.getRootAsMessage(
                  java.nio.ByteBuffer.wrap(partBytes, o + 8, metaLen));
          assertThat(msg.headerType()).isEqualTo(org.apache.arrow.flatbuf.MessageHeader.RecordBatch);
          long bodyLen = msg.bodyLength();
          long afterMeta = o + 8L + metaLen;
          long bodyStart = (afterMeta + 7) / 8 * 8; // 8 字节对齐
          assertThat(length).isEqualTo(bodyStart + bodyLen - o);

          // ② schema + 该 batch + EOS → 顺序读，行值与顺序整读逐行一致
          java.io.ByteArrayOutputStream mini = new java.io.ByteArrayOutputStream();
          mini.write(schemaMessage, 0, schemaMessage.length);
          mini.write(partBytes, o, (int) length);
          mini.write(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0, 0, 0, 0});
          try (ArrowStreamReader reader =
              new ArrowStreamReader(new java.io.ByteArrayInputStream(mini.toByteArray()), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(reader.loadNextBatch()).isTrue(); // 恰一个 batch，随后 EOS
            assertThat(root.getRowCount()).isEqualTo(t.get(2).asLong());
            for (int i = 0; i < root.getRowCount(); i++) {
              random.add(root.getVector("id").getObject(i) instanceof Number n ? n.longValue() : -1L);
            }
            assertThat(reader.loadNextBatch()).isFalse();
          }
        }
        assertThat(random).isEqualTo(sequential);
      }
    }
  }
}
