package org.hypertrace.entity.service.client.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link EntityServiceClientConfig}
 */
public class EntityServiceClientConfigTest {

  @Test
  public void testEntityServiceClientConfigFromConfig() {
    Map<String, Object> entityServiceClientConfigMap = new HashMap<>();
    Map<String, Object> map = new HashMap<>();
    map.put("host", "localhost");
    map.put("port", 50061);
    entityServiceClientConfigMap.put("entity.service.config", map);
    Config config = ConfigFactory.parseMap(entityServiceClientConfigMap);

    EntityServiceClientConfig entityServiceClientConfig = EntityServiceClientConfig.from(config);
    Assertions.assertEquals("localhost", entityServiceClientConfig.getHost());
    Assertions.assertEquals(50061, entityServiceClientConfig.getPort());
  }

  @Test
  public void testEntityServiceClientConfigInvalid() {
    Map<String, Object> map = new HashMap<>();
    map.put("host", "localhost");
    map.put("port", 50061);
    Config config = ConfigFactory.parseMap(map);
    Assertions.assertThrows(ConfigException.Missing.class, () -> {
      EntityServiceClientConfig.from(config);
    });
  }
}
