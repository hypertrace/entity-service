package org.hypertrace.entity.data.service.client;

import com.typesafe.config.Config;

/**
 * Interface to be implemented by the providers which will create EDSClient based on config.
 */
public interface EntityDataServiceClientProvider {

  EdsClient createClient(Config config);
}
