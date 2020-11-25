package org.hypertrace.entity.service.client.config;

import com.typesafe.config.Config;

/**
 * Config class for all the EntityService clients
 * <p>
 * Any service that needs to use one of the EntityService clients is expected to have a config like
 * below:
 * <p>
 * entity.service.config = { host = "localhost" port = 50061 }
 */
public class EntityServiceClientConfig {

  private static final String ENTITY_SERVICE_CONFIG_KEY = "entity.service.config";
  private final String host;
  private final int port;
  private final EntityServiceClientCacheConfig cacheConfig;

  private EntityServiceClientConfig(Config clientConfig) {
    this.host = clientConfig.getString("host");
    this.port = clientConfig.getInt("port");
    this.cacheConfig = clientConfig.hasPath("cache") ?
        new EntityServiceClientCacheConfig(clientConfig.getConfig("cache"))
        : EntityServiceClientCacheConfig.DEFAULT;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public EntityServiceClientCacheConfig getCacheConfig() {
    return cacheConfig;
  }

  public static EntityServiceClientConfig from(Config config) {
    return new EntityServiceClientConfig(config.getConfig(ENTITY_SERVICE_CONFIG_KEY));
  }

  @Override
  public String toString() {
    return "EntityServiceClientConfig{" +
        "host='" + host + '\'' +
        ", port=" + port +
        '}';
  }
}
