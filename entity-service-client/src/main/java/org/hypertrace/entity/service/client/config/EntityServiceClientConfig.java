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

  private String host;
  private int port;

  private EntityServiceClientConfig(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public static EntityServiceClientConfig from(Config config) {
    Config entityServiceConfig = config.getConfig(ENTITY_SERVICE_CONFIG_KEY);
    return new EntityServiceClientConfig(
        entityServiceConfig.getString("host"), entityServiceConfig.getInt("port"));
  }

  @Override
  public String toString() {
    return "EntityServiceClientConfig{" +
        "host='" + host + '\'' +
        ", port=" + port +
        '}';
  }
}
