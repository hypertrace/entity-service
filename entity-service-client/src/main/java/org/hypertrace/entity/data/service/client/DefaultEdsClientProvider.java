package org.hypertrace.entity.data.service.client;

import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.hypertrace.entity.service.client.config.EntityServiceClientConfig;

/**
 * Default implementation of {@link EntityDataServiceClientProvider} which instantiates EDS client
 * to communicate with EDS.
 */
public class DefaultEdsClientProvider implements EntityDataServiceClientProvider {

  @Override
  public EdsClient createClient(Config config) {
    EntityServiceClientConfig entityServiceClientConfig = EntityServiceClientConfig.from(config);
    return new EdsCacheClient(new EntityDataServiceClient(ManagedChannelBuilder
        .forAddress(entityServiceClientConfig.getHost(), entityServiceClientConfig.getPort())
        .usePlaintext().build()),
        entityServiceClientConfig.getCacheConfig());
  }

  @Override
  public EdsClient createClient(Channel channel, Config config) {
    EntityServiceClientConfig entityServiceClientConfig = EntityServiceClientConfig.from(config);
    return new EdsCacheClient(new EntityDataServiceClient(channel),
        entityServiceClientConfig.getCacheConfig());
  }
}
