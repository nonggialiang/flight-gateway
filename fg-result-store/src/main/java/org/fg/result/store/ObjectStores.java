package org.fg.result.store;

import io.minio.MinioClient;
import org.fg.common.config.GatewayConfig;

/** 从 GatewayConfig 构建 ObjectStoreService。 */
public final class ObjectStores {

  private ObjectStores() {}

  public static ObjectStoreService fromConfig(GatewayConfig config) {
    MinioClient client =
        MinioClient.builder()
            .endpoint(config.getString("fg.result.s3.endpoint"))
            .credentials(
                config.getString("fg.result.s3.access-key"),
                config.getString("fg.result.s3.secret-key"))
            .build();
    return new ObjectStoreService(client, config.getString("fg.result.bucket"));
  }
}
