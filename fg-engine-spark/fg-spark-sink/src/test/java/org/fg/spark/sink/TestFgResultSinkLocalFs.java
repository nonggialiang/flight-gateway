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
            "{\"objectUri\":\"" + dir + "\",\"zstdCompression\":false,\"maxRecordsPerBatchHint\":256}")
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
}
