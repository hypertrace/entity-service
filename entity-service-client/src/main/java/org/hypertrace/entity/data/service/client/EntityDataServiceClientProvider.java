package org.hypertrace.entity.data.service.client;

import com.typesafe.config.Config;
import io.grpc.Channel;

/**
 * Interface to be implemented by the providers which will create EDSClient based on config.
 */
public interface EntityDataServiceClientProvider {

  @Deprecated (forRemoval = true)
  EdsClient createClient(Config config);
  EdsClient createClient(Channel channel, Config config);
}
