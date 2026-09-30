package org.fg.result.store;

import io.minio.MinioClient;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import org.fg.common.config.GatewayConfig;

/** 从 GatewayConfig 构建 ObjectStoreService。 */
public final class ObjectStores {

  private ObjectStores() {}

  public static ObjectStoreService fromConfig(GatewayConfig config) {
    // 显式 HTTP 超时（2026-09-30 DBeaver 实证）：MinIO 8.5 默认 HttpClient 无有效
    // read 超时——慢/挂的对象 GET 会把 relay 线程无限期阻塞（fg.relay.client.readiness
    // .timeout 只管 batch 间背压，管不到 loadNextBatch 的 socket 读），最后由网络层
    // Connection reset 兜底（实测挂 2m19s）。read=socket 级（非总时长，大对象慢网络
    // 安全），call=整请求护栏。
    long connectMs = config.hasPath("fg.result.s3.connect-timeout")
        ? config.getDurationMs("fg.result.s3.connect-timeout") : 10_000L;
    long readMs = config.hasPath("fg.result.s3.read-timeout")
        ? config.getDurationMs("fg.result.s3.read-timeout") : 60_000L;
    long callMs = config.hasPath("fg.result.s3.call-timeout")
        ? config.getDurationMs("fg.result.s3.call-timeout") : 600_000L;
    OkHttpClient http =
        new OkHttpClient.Builder()
            .connectTimeout(connectMs, TimeUnit.MILLISECONDS)
            .readTimeout(readMs, TimeUnit.MILLISECONDS)
            .writeTimeout(60_000L, TimeUnit.MILLISECONDS)
            .callTimeout(callMs, TimeUnit.MILLISECONDS)
            .build();
    MinioClient client =
        MinioClient.builder()
            .endpoint(config.getString("fg.result.s3.endpoint"))
            .credentials(
                config.getString("fg.result.s3.access-key"),
                config.getString("fg.result.s3.secret-key"))
            .httpClient(http)
            .build();
    return new ObjectStoreService(client, config.getString("fg.result.bucket"));
  }
}
