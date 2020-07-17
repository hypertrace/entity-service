package org.hypertrace.entity.service.client.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link EntityServiceClientCacheConfig}
 * */
public class EntityServiceClientCacheConfigTest {

  @Test
  public void testEntityServiceClientCacheConfigFromConfig() {
    Map<String, Object> entityServiceClientConfigMap = new HashMap<>();
    Map<String, Object> map = new HashMap<>();
    map.put("host", "localhost");
    map.put("port", 50061);

    Map<String, Integer> cacheConfigMap = new HashMap<>();
    cacheConfigMap.put("entity.cache.expiry.ms", 5000);
    cacheConfigMap.put("entity.max.cache.size", 10000);
    cacheConfigMap.put("enriched.entity.cache.expiry.ms", 6000);
    cacheConfigMap.put("enriched.entity.max.cache.size", 20000);
    cacheConfigMap.put("entity.ids.cache.expiry.ms", 7000);
    cacheConfigMap.put("entity.ids.max.cache.size", 30000);
    map.put("cache", cacheConfigMap);


    entityServiceClientConfigMap.put("entity.service.config", map);
    Config config = ConfigFactory.parseMap(entityServiceClientConfigMap);

    EntityServiceClientConfig entityServiceClientConfig = EntityServiceClientConfig.from(config);
    EntityServiceClientCacheConfig cacheConfig = entityServiceClientConfig.getCacheConfig();
    Assertions.assertEquals(5000, cacheConfig.getEntityCacheExpiryMs());
    Assertions.assertEquals(10000, cacheConfig.getEntityMaxCacheSize());
    Assertions.assertEquals(6000, cacheConfig.getEnrichedEntityCacheExpiryMs());
    Assertions.assertEquals(20000, cacheConfig.getEnrichedEntityMaxCacheSize());
    Assertions.assertEquals(7000, cacheConfig.getEntityIdsCacheExpiryMs());
    Assertions.assertEquals(30000, cacheConfig.getEntityIdsMaxCacheSize());

  }

  @Test
  public void testEntityServiceClientDefaultCacheConfigFromConfig() {
    Map<String, Object> entityServiceClientConfigMap = new HashMap<>();
    Map<String, Object> map = new HashMap<>();
    map.put("host", "localhost");
    map.put("port", 50061);
    entityServiceClientConfigMap.put("entity.service.config", map);
    Config config = ConfigFactory.parseMap(entityServiceClientConfigMap);

    EntityServiceClientConfig entityServiceClientConfig = EntityServiceClientConfig.from(config);
    EntityServiceClientCacheConfig cacheConfig = entityServiceClientConfig.getCacheConfig();
    Assertions.assertEquals(300000L, cacheConfig.getEntityCacheExpiryMs());
    Assertions.assertEquals(1000, cacheConfig.getEntityMaxCacheSize());
    Assertions.assertEquals(300000L, cacheConfig.getEnrichedEntityCacheExpiryMs());
    Assertions.assertEquals(1000, cacheConfig.getEnrichedEntityMaxCacheSize());
    Assertions.assertEquals(300000L, cacheConfig.getEntityIdsCacheExpiryMs());
    Assertions.assertEquals(1000, cacheConfig.getEntityIdsMaxCacheSize());

  }

}
