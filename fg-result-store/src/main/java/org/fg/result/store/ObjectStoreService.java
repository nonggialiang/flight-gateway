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
import okhttp3.HttpUrl;
import org.fg.result.manifest.ResultManifest;

/**
 * 结果对象访问（MinIO）：manifest 读取、对象 stat、presigned GET、前缀清除。gateway 零结果
 * 状态——所有寻址经 manifest（design §4.6）。
 */
public final class ObjectStoreService implements AutoCloseable {

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

  /** 按清单删除 part + manifest（retention 清扫/取消善后可选）。 */
  public void purgeResult(String resultKeyPrefix) {
    List<DeleteObject> toDelete = new LinkedList<>();
    try {
      ResultManifest manifest = readManifest(resultKeyPrefix);
      manifest.parts().forEach(p -> toDelete.add(new DeleteObject(objectName(p.uri()))));
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

  /** part uri（如 s3://bucket/key 或纯 key）→ object key。 */
  public static String objectName(String partUri) {
    HttpUrl url = HttpUrl.parse(partUri);
    if (url != null) {
      // s3:// style: host = bucket
      String encoded = url.encodedPath();
      return encoded.startsWith("/") ? encoded.substring(1) : encoded;
    }
    return partUri;
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
