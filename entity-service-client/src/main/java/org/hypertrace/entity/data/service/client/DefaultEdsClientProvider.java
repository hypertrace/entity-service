package org.hypertrace.entity.data.service.client;

import com.typesafe.config.Config;
import org.hypertrace.entity.service.client.config.EntityServiceClientConfig;

/**
 * Default implementation of {@link EntityDataServiceClientProvider} which instantiates EDS client
 * to communicate with EDS.
 */
public class DefaultEdsClientProvider implements EntityDataServiceClientProvider {

  @Override
  public EdsClient createClient(Config config) {
    return new EdsCacheClient(EntityServiceClientConfig.from(config));
  }
}
