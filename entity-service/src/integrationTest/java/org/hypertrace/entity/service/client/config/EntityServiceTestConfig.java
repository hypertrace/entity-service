package org.hypertrace.entity.service.client.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.serviceframework.config.IntegrationTestConfigClientFactory;

/**
 * Entity Service Client config used by integration tests
 */
public class EntityServiceTestConfig {

  public static EntityServiceClientConfig getClientConfig() {
    Config serviceConfig = IntegrationTestConfigClientFactory
        .getConfigClientForService("entity-service")
        .getConfig();
    Map<String, Object> entityServiceClientConfigMap = new HashMap<>();
    Map<String, Object> map = new HashMap<>();
    map.put("host", "localhost");
    map.put("port", serviceConfig.getInt("service.port"));
    entityServiceClientConfigMap.put("entity.service.config", map);
    Config config = ConfigFactory.parseMap(entityServiceClientConfigMap);
    return EntityServiceClientConfig.from(config);
  }
}
