package org.fg.spark.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.util.Base64;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * manifest 聚合与写入（driver，对标 Iceberg SnapshotSummary，design §4.4.2/§4.6）：
 *
 * <ul>
 *   <li>commit：聚合 PartMetadata → manifest.json 上传；part 顺序 = partition index（H4）；
 *       空分区（uri=null）跳过；
 *   <li>abort：按精确清单删已写 part（D6）。
 * </ul>
 */
public final class ManifestWriter {

  private static final Logger logger = LoggerFactory.getLogger(ManifestWriter.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String TIMEZONE = "UTC";

  private ManifestWriter() {}

  public static void write(SpecOptions spec, StructType schema, PartMetadata[] parts) {
    try {
      ObjectNode manifest = MAPPER.createObjectNode();
      manifest.put("queryId", queryIdOf(spec.objectUri()));
      manifest.put("schemaBase64", Base64.getEncoder().encodeToString(serializeSchema(schema)));
      ArrayNode partsNode = manifest.putArray("parts");
      long totalRows = 0;
      long totalBytes = 0;
      for (PartMetadata p : parts) {
        if (p.uri() == null) {
          continue; // 空分区不产文件
        }
        ObjectNode n = partsNode.addObject();
        n.put("uri", p.uri());
        n.put("index", p.partitionIndex());
        n.put("recordCount", p.recordCount());
        n.put("bytes", p.bytes());
        n.put("attempt", p.attempt());
        totalRows += p.recordCount();
        totalBytes += p.bytes();
      }
      manifest.put("rowCount", totalRows);
      manifest.put("bytes", totalBytes);
      manifest.put("createdAtEpochMs", System.currentTimeMillis());

      Configuration conf = hadoopConf();
      Path path = new Path(URI.create(spec.objectUri() + "manifest.json"));
      try (FileSystem fs = path.getFileSystem(conf);
          OutputStream out = fs.create(path, true)) {
        out.write(MAPPER.writeValueAsBytes(manifest));
      }
      logger.info(
          "Manifest written: {} parts={} rows={} bytes={}",
          path,
          parts.length,
          totalRows,
          totalBytes);
    } catch (Exception e) {
      throw new RuntimeException("Manifest commit failed for " + spec.objectUri(), e);
    }
  }

  /** abort：按精确清单删已写 part（retention 兜底双保险之一）。 */
  public static void abort(SpecOptions spec, PartMetadata[] messages) {
    try {
      Configuration conf = hadoopConf();
      int deleted = 0;
      for (PartMetadata p : messages) {
        if (p.uri() == null) {
          continue;
        }
        Path path = new Path(URI.create(p.uri()));
        try (FileSystem fs = path.getFileSystem(conf)) {
          if (fs.exists(path) && fs.delete(path, false)) {
            deleted++;
          }
        }
      }
      logger.info("Abort cleanup: deleted {} parts for {}", deleted, spec.objectUri());
    } catch (Exception e) {
      logger.warn("Abort cleanup failed for {}: {}", spec.objectUri(), e.toString());
    }
  }

  private static Configuration hadoopConf() {
    return org.apache.spark.SparkContext.getOrCreate().hadoopConfiguration();
  }

  /** objectUri 形如 s3://bucket/prefix/{engine}/{user}/{queryId}/ → queryId。 */
  static String queryIdOf(String objectUri) {
    String trimmed = objectUri.endsWith("/") ? objectUri.substring(0, objectUri.length() - 1) : objectUri;
    int lastSlash = trimmed.lastIndexOf('/');
    return lastSlash >= 0 ? trimmed.substring(lastSlash + 1) : trimmed;
  }

  private static byte[] serializeSchema(StructType schema) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (WriteChannel channel = new WriteChannel(Channels.newChannel(out))) {
      MessageSerializer.serialize(channel, ArrowUtils.toArrowSchema(schema, TIMEZONE, true, false));
    }
    return out.toByteArray();
  }
}
