package org.fg.result.store;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectsArgs;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.messages.DeleteObject;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.fg.result.manifest.ResultManifest;

/**
 * 结果对象访问（MinIO）：manifest 读取、对象 stat、presigned GET、前缀清除。gateway 零结果
 * 状态——所有寻址经 manifest（design §4.6）。
 */
public final class ObjectStoreService implements AutoCloseable {

  /** D27：per-part batch 索引边车后缀（与 sink 侧 FgResultSinks.BATCH_INDEX_SUFFIX 同值——
   * 网关按 part uri 推导边车 key，缺席回落整 part 顺序读）。 */
  public static final String BATCH_INDEX_SUFFIX = ".bidx";

  private final MinioClient client;
  private final String bucket;

  public ObjectStoreService(MinioClient client, String bucket) {
    this.client = client;
    this.bucket = bucket;
  }

  public String bucket() {
    return bucket;
  }

  public boolean manifestExists(String resultKeyPrefix) {
    try {
      statObject(objectKey(resultKeyPrefix, "manifest.json"));
      return true;
    } catch (RuntimeException e) {
      return false;
    }
  }

  public ResultManifest readManifest(String resultKeyPrefix) {
    try (InputStream in =
        client
            .getObject(GetObjectArgs.builder().bucket(bucket).object(manifestKey(resultKeyPrefix)).build())) {
      return ResultManifest.readAllBytesThenParse(in);
    } catch (Exception e) {
      throw new IllegalStateException("Unable to read manifest at " + manifestKey(resultKeyPrefix), e);
    }
  }

  public StatObjectResponse statObject(String objectKey) {
    try {
      return client.statObject(StatObjectArgs.builder().bucket(bucket).object(objectKey).build());
    } catch (Exception e) {
      throw new IllegalStateException("Unable to stat " + objectKey, e);
    }
  }

  /** presigned GET URL（design §4.6：presign 前 stat 校验）。 */
  public String presignGet(String objectKey, int ttlSeconds) {
    statObject(objectKey); // existence/size check first
    try {
      return client.getPresignedObjectUrl(
          io.minio.GetPresignedObjectUrlArgs.builder()
              .method(io.minio.http.Method.GET)
              .bucket(bucket)
              .object(objectKey)
              .expiry(ttlSeconds, TimeUnit.SECONDS)
              .build());
    } catch (Exception e) {
      throw new IllegalStateException("Unable to presign " + objectKey, e);
    }
  }

  /** 获取对象原始输入流（relay DoGet 用）。调用方负责关闭。 */
  public InputStream getObject(String objectKey) {
    try {
      return client
          .getObject(GetObjectArgs.builder().bucket(bucket).object(objectKey).build());
    } catch (Exception e) {
      throw new IllegalStateException("Unable to get " + objectKey, e);
    }
  }

  /** 按清单删除 part + manifest + batch 索引边车（D27：{part}.bidx 连带）。 */
  public void purgeResult(String resultKeyPrefix) {
    List<DeleteObject> toDelete = new LinkedList<>();
    try {
      ResultManifest manifest = readManifest(resultKeyPrefix);
      manifest.parts().forEach(p -> {
        toDelete.add(new DeleteObject(objectName(p.uri())));
        toDelete.add(new DeleteObject(objectName(p.uri()) + BATCH_INDEX_SUFFIX));
      });
    } catch (RuntimeException e) {
      // no manifest (orphan or aborted); fall through to delete manifest itself if present
    }
    toDelete.add(new DeleteObject(manifestKey(resultKeyPrefix)));
    try {
      for (io.minio.Result<io.minio.messages.DeleteError> ignored :
          client.removeObjects(
              RemoveObjectsArgs.builder().bucket(bucket).objects(toDelete).build())) {
        // lazy iterable; consume to drive deletion
      }
    } catch (Exception e) {
      throw new IllegalStateException("Unable to purge " + resultKeyPrefix, e);
    }
  }

  /**
   * D27 Range GET（batch 级随机读）：取对象 [offset, offset+length) 字节——页切片按
   * .bidx 三元组取单 encapsulated message，不触碰对象其余部分。
   */
  public InputStream getObjectRange(String objectKey, long offset, long length) {
    try {
      return client.getObject(
          GetObjectArgs.builder()
              .bucket(bucket)
              .object(objectKey)
              .offset(offset)
              .length(length)
              .build());
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to get range " + objectKey + "[" + offset + "," + (offset + length) + ")", e);
    }
  }

  /**
   * D27：读 part 的 batch 索引边车（{part}.bidx）。<b>缺席/任何异常均返回 null</b>——
   * 边车是优化不是正确性依赖，调用方回落整 part 顺序读（真实故障会在回落路径浮出）。
   */
  public org.fg.result.manifest.BatchIndex readBatchIndex(String partObjectKey) {
    try (InputStream in = getObject(partObjectKey + BATCH_INDEX_SUFFIX)) {
      return org.fg.result.manifest.BatchIndex.parse(in.readAllBytes());
    } catch (Exception e) {
      return null;
    }
  }

  /** part uri（如 s3a://bucket/key 或纯 key）→ object key（去 scheme+bucket，仅留 key）。 */
  public static String objectName(String partUri) {
    if (!partUri.contains("://")) {
      return partUri;
    }
    try {
      java.net.URI uri = java.net.URI.create(partUri);
      String path = uri.getPath() == null ? "" : uri.getPath();
      return path.startsWith("/") ? path.substring(1) : path;
    } catch (Exception e) {
      return partUri;
    }
  }

  public static String objectKey(String resultKeyPrefix, String name) {
    return resultKeyPrefix + "/" + name;
  }

  private static String manifestKey(String resultKeyPrefix) {
    return objectKey(resultKeyPrefix, "manifest.json");
  }

  @Override
  public void close() {
    // MinioClient holds an OkHttpClient; no explicit close API in 8.5.x
  }
}
