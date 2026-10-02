package org.fg.result.store;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.fg.result.manifest.ResultManifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * 结果对象访问（D28 改造：AWS SDK v2 S3 通用客户端——MinIO 经 endpointOverride +
 * path-style 兼容，真 S3/OSS/R2 直接可用）。manifest 读取、对象 stat、presigned GET、
 * 前缀清除、Range GET。gateway 零结果状态——所有寻址经 manifest（design §4.6）。
 *
 * <p>Range 读挂 {@link RangeCache} 两级缓存（Caffeine 内存索引 + 异步磁盘层）：scroll
 * 翻页重复段零网络；内存上限与磁盘目录/总量均配置（fg.result.range-cache.*）。快照不可变，
 * 两层无需失效协议。同步流（relay 整 part / manifest / bidx）不走缓存（一次性读，缓存无益）。
 */
public final class ObjectStoreService implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStoreService.class);

  /** D27：per-part batch 索引边车后缀（与 sink 侧 FgResultSinks.BATCH_INDEX_SUFFIX 同值——
   * 网关按 part uri 推导边车 key，缺席回落整 part 顺序读）。 */
  public static final String BATCH_INDEX_SUFFIX = ".bidx";

  private final S3Client client;
  private final S3Presigner presigner;
  private final String bucket;
  private final RangeCache rangeCache;
  /** Range 加载执行器（同步 ranged GET 的异步包装）。 */
  private final java.util.concurrent.ExecutorService rangeExecutor;

  /** 生产构造（ObjectStores.fromConfig）。timeoutMs：HTTP 超时（connect/read/write，毫秒）。 */
  public ObjectStoreService(
      String endpoint, String accessKey, String secretKey, String region, String bucket,
      long rangeCacheMaxMemoryBytes, Path rangeCacheDiskDir, long rangeCacheDiskMaxBytes,
      long connectTimeoutMs, long readTimeoutMs, long writeTimeoutMs) {
    this.bucket = bucket;
    var credentials = software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
        accessKey, secretKey);
    var provider =
        software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(credentials);
    var regionVal = Region.of(region == null || region.isBlank() ? "us-east-1" : region);

    // HTTP 超时（2026-09-30 DBeaver 实证教训延续：慢/挂的对象读必须护栏）。
    // 客户端选型（实证教训 2026-10-02）：awssdk netty-nio-client(4.1.118) 与
    // arrow-memory-netty-buffer-patch(4.2.9) 类路径冲突（AbstractMethodError
    // safeInitializeRawCnt）——RangeCache 的异步加载经同步 S3Client(apache) +
    // 专用线程池包装，与 arrow 共存且无第二套 netty。
    var syncHttp = software.amazon.awssdk.http.apache.ApacheHttpClient.builder()
        .connectionTimeout(Duration.ofMillis(connectTimeoutMs))
        .socketTimeout(Duration.ofMillis(readTimeoutMs));
    var syncBuilder = S3Client.builder().region(regionVal).credentialsProvider(provider)
        .forcePathStyle(true) // MinIO 兼容；真 S3 亦接受
        .httpClient(syncHttp.build());
    var presignerBuilder = S3Presigner.builder().region(regionVal).credentialsProvider(provider)
        .serviceConfiguration(
            software.amazon.awssdk.services.s3.S3Configuration.builder()
                .pathStyleAccessEnabled(true).build());
    if (endpoint != null && !endpoint.isBlank()) {
      URI uri = URI.create(endpoint);
      syncBuilder.endpointOverride(uri);
      presignerBuilder.endpointOverride(uri);
    }
    this.client = syncBuilder.build();
    this.presigner = presignerBuilder.build();
    this.rangeExecutor = java.util.concurrent.Executors.newFixedThreadPool(4, r -> {
      Thread t = new Thread(r, "fg-s3-range");
      t.setDaemon(true);
      return t;
    });
    this.rangeCache = new RangeCache(
        rangeCacheMaxMemoryBytes, rangeCacheDiskDir, rangeCacheDiskMaxBytes,
        (key, offset, length) -> java.util.concurrent.CompletableFuture.supplyAsync(
            () -> {
              try (InputStream in = client.getObject(
                  GetObjectRequest.builder().bucket(bucket).key(key)
                      .range("bytes=" + offset + "-" + (offset + length - 1)).build())) {
                return in.readAllBytes();
              } catch (Exception e) {
                throw new IllegalStateException(
                    "Unable to get range " + key + "[" + offset + "," + (offset + length) + ")", e);
              }
            },
            rangeExecutor));
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
    try (InputStream in = client.getObject(
        GetObjectRequest.builder().bucket(bucket).key(manifestKey(resultKeyPrefix)).build())) {
      return ResultManifest.readAllBytesThenParse(in);
    } catch (Exception e) {
      throw new IllegalStateException("Unable to read manifest at " + manifestKey(resultKeyPrefix), e);
    }
  }

  /** 对象存在性/元数据（presign 前校验等内部用；NoSuchKey 抛 RuntimeException）。 */
  public HeadObjectResponse statObject(String objectKey) {
    try {
      return client.headObject(
          HeadObjectRequest.builder().bucket(bucket).key(objectKey).build());
    } catch (NoSuchKeyException e) {
      throw new IllegalStateException("No such object: " + objectKey, e);
    } catch (Exception e) {
      throw new IllegalStateException("Unable to stat " + objectKey, e);
    }
  }

  /** presigned GET URL（design §4.6：presign 前 stat 校验）。 */
  public String presignGet(String objectKey, int ttlSeconds) {
    statObject(objectKey); // existence/size check first
    try {
      return presigner.presignGetObject(
              GetObjectPresignRequest.builder()
                  .signatureDuration(Duration.ofSeconds(ttlSeconds))
                  .getObjectRequest(
                      GetObjectRequest.builder().bucket(bucket).key(objectKey).build())
                  .build())
          .url()
          .toString();
    } catch (Exception e) {
      throw new IllegalStateException("Unable to presign " + objectKey, e);
    }
  }

  /** 获取对象原始输入流（relay DoGet / bidx 读取用）。调用方负责关闭。 */
  public InputStream getObject(String objectKey) {
    try {
      return client.getObject(
          GetObjectRequest.builder().bucket(bucket).key(objectKey).build());
    } catch (Exception e) {
      throw new IllegalStateException("Unable to get " + objectKey, e);
    }
  }

  /** 按清单删除 part + manifest + batch 索引边车（D27：{part}.bidx 连带）。 */
  public void purgeResult(String resultKeyPrefix) {
    List<ObjectIdentifier> toDelete = new ArrayList<>();
    try {
      ResultManifest manifest = readManifest(resultKeyPrefix);
      manifest.parts().forEach(p -> {
        toDelete.add(ObjectIdentifier.builder().key(objectName(p.uri())).build());
        toDelete.add(ObjectIdentifier.builder().key(objectName(p.uri()) + BATCH_INDEX_SUFFIX).build());
      });
    } catch (RuntimeException e) {
      // no manifest (orphan or aborted); fall through to delete manifest itself if present
    }
    toDelete.add(ObjectIdentifier.builder().key(manifestKey(resultKeyPrefix)).build());
    try {
      client.deleteObjects(DeleteObjectsRequest.builder().bucket(bucket).delete(
          Delete.builder().objects(toDelete).build()).build());
    } catch (Exception e) {
      throw new IllegalStateException("Unable to purge " + resultKeyPrefix, e);
    }
  }

  /**
   * D27/D28 Range GET（batch 级随机读）：取对象 [offset, offset+length) 字节，经
   * {@link RangeCache}（内存 → 磁盘 → S3，S3 成功异步落盘）。异步返回，快照不可变缓存安全。
   */
  public CompletableFuture<byte[]> readRange(String objectKey, long offset, int length) {
    return rangeCache.read(objectKey, offset, length);
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
    rangeCache.close();
    rangeExecutor.shutdown();
    client.close();
    presigner.close();
  }
}
