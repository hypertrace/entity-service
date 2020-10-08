package org.hypertrace.entity.query.service.client;

import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EntityCacheKeyTest {
  @Test
  public void testEntityCacheKey() {
    EntityCacheKey<String> entityCacheKey1 = new EntityCacheKey<>("key1", "tenantId1",
        Map.of("k1", "v1", "k2", "v2"));
    EntityCacheKey<String> entityCacheKey2 = new EntityCacheKey<>("key2", "tenantId1",
        Map.of("k1", "v1", "k2", "v2"));
    EntityCacheKey<String> entityCacheKey3 = new EntityCacheKey<>("key1", "tenantId2",
        Map.of("k1", "v1", "k2", "v2"));
    EntityCacheKey<String> entityCacheKey4 = new EntityCacheKey<>("key1", "tenantId1",
        Map.of("k1", "v1"));
    EntityCacheKey<String> entityCacheKey5 = new EntityCacheKey<>("key1", "tenantId1",
        Map.of("k1", "v1", "k2", "v2"));

    Assertions.assertEquals("key1", entityCacheKey1.getDataKey());
    Assertions.assertEquals(Map.of("k1", "v1", "k2", "v2"),
        entityCacheKey1.getHeaders());

    Assertions.assertEquals(entityCacheKey1, entityCacheKey1);
    Assertions.assertNotEquals(entityCacheKey1, entityCacheKey2);
    Assertions.assertNotEquals(entityCacheKey1, entityCacheKey3);
    Assertions.assertEquals(entityCacheKey1, entityCacheKey4);
    Assertions.assertEquals(entityCacheKey1, entityCacheKey5);
    Assertions.assertNotEquals(entityCacheKey1, null);
    Assertions.assertNotEquals(entityCacheKey1, 5);

    Assertions.assertEquals(Objects.hashCode(entityCacheKey1), Objects.hashCode(entityCacheKey5));
    Assertions.assertEquals(Objects.hashCode(entityCacheKey1), Objects.hashCode(entityCacheKey4));
    Assertions.assertNotEquals(Objects.hashCode(entityCacheKey1), Objects.hashCode(entityCacheKey3));
  }
}
