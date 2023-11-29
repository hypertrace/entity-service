package org.hypertrace.entity.service.client.config;

import com.typesafe.config.Config;

/**
 * Config class for cache config for different entity related caches at EntityService clients e.g
 * entity.service.config = { cache = { entity.cache.expiry.ms = 30000 entity.max.cache.size = 1000
 * enriched.entity.cache.expiry.ms = 40000 enriched.entity.max.cache.size = 2000
 * entity.ids.cache.expiry.ms = 50000 entity.ids.max.cache.size = 3000 } }
 */
public class EntityServiceClientCacheConfig {

  public static long DEFAULT_CACHE_REFRESH_MS = 240000L;
  public static long DEFAULT_CACHE_EXPIRY_MS = 300000L;
  public static long DEFAULT_MAX_CACHE_SIZE = 1000L;

  private static final String ENTITY_CACHE_REFRESH_MS = "entity.cache.refresh.ms";
  private static final String ENTITY_CACHE_EXPIRY_MS = "entity.cache.expiry.ms";
  private static final String ENTITY_MAX_CACHE_SIZE = "entity.max.cache.size";

  private static final String ENRICHED_ENTITY_CACHE_REFRESH_MS = "enriched.entity.cache.refresh.ms";
  private static final String ENRICHED_ENTITY_CACHE_EXPIRY_MS = "enriched.entity.cache.expiry.ms";
  private static final String ENRICHED_ENTITY_MAX_CACHE_SIZE = "enriched.entity.max.cache.size";

  private static final String ENTITY_IDS_CACHE_REFRESH_MS = "entity.ids.cache.refresh.ms";
  private static final String ENTITY_IDS_CACHE_EXPIRY_MS = "entity.ids.cache.expiry.ms";
  private static final String ENTITY_IDS_MAX_CACHE_SIZE = "entity.ids.max.cache.size";
  private static final String ENTITY_CACHE_EXPIRE_AFTER_ACCESS_MODE =
      "entity.cache.expire.after.access"; // not used for enriched entity cache, applies to other
  // two

  public static final EntityServiceClientCacheConfig DEFAULT = new EntityServiceClientCacheConfig();

  private final long entityCacheRefreshMs;
  private final long entityCacheExpiryMs;
  private final long entityMaxCacheSize;
  private final long enrichedEntityCacheRefreshMs;
  private final long enrichedEntityCacheExpiryMs;
  private final long enrichedEntityMaxCacheSize;
  private final long entityIdsCacheRefreshMs;
  private final long entityIdsCacheExpiryMs;
  private final long entityIdsMaxCacheSize;
  private final boolean isExpiredAfterAccess;

  public EntityServiceClientCacheConfig(Config clientCacheConfig) {
    entityCacheRefreshMs =
        clientCacheConfig.hasPath(ENTITY_CACHE_REFRESH_MS)
            ? clientCacheConfig.getLong(ENTITY_CACHE_REFRESH_MS)
            : DEFAULT_CACHE_REFRESH_MS;
    entityCacheExpiryMs =
        clientCacheConfig.hasPath(ENTITY_CACHE_EXPIRY_MS)
            ? clientCacheConfig.getLong(ENTITY_CACHE_EXPIRY_MS)
            : DEFAULT_CACHE_EXPIRY_MS;
    entityMaxCacheSize =
        clientCacheConfig.hasPath(ENTITY_MAX_CACHE_SIZE)
            ? clientCacheConfig.getLong(ENTITY_MAX_CACHE_SIZE)
            : DEFAULT_MAX_CACHE_SIZE;

    enrichedEntityCacheRefreshMs =
        clientCacheConfig.hasPath(ENRICHED_ENTITY_CACHE_REFRESH_MS)
            ? clientCacheConfig.getLong(ENRICHED_ENTITY_CACHE_REFRESH_MS)
            : DEFAULT_CACHE_REFRESH_MS;
    enrichedEntityCacheExpiryMs =
        clientCacheConfig.hasPath(ENRICHED_ENTITY_CACHE_EXPIRY_MS)
            ? clientCacheConfig.getLong(ENRICHED_ENTITY_CACHE_EXPIRY_MS)
            : DEFAULT_CACHE_EXPIRY_MS;
    enrichedEntityMaxCacheSize =
        clientCacheConfig.hasPath(ENRICHED_ENTITY_MAX_CACHE_SIZE)
            ? clientCacheConfig.getLong(ENRICHED_ENTITY_MAX_CACHE_SIZE)
            : DEFAULT_MAX_CACHE_SIZE;

    entityIdsCacheRefreshMs =
        clientCacheConfig.hasPath(ENTITY_IDS_CACHE_REFRESH_MS)
            ? clientCacheConfig.getLong(ENTITY_IDS_CACHE_REFRESH_MS)
            : DEFAULT_CACHE_REFRESH_MS;
    entityIdsCacheExpiryMs =
        clientCacheConfig.hasPath(ENTITY_IDS_CACHE_EXPIRY_MS)
            ? clientCacheConfig.getLong(ENTITY_IDS_CACHE_EXPIRY_MS)
            : DEFAULT_CACHE_EXPIRY_MS;
    entityIdsMaxCacheSize =
        clientCacheConfig.hasPath(ENTITY_IDS_MAX_CACHE_SIZE)
            ? clientCacheConfig.getLong(ENTITY_IDS_MAX_CACHE_SIZE)
            : DEFAULT_MAX_CACHE_SIZE;
    isExpiredAfterAccess =
        clientCacheConfig.hasPath(ENTITY_CACHE_EXPIRE_AFTER_ACCESS_MODE)
            && clientCacheConfig.getBoolean(ENTITY_CACHE_EXPIRE_AFTER_ACCESS_MODE);
  }

  public EntityServiceClientCacheConfig() {
    entityCacheRefreshMs = DEFAULT_CACHE_REFRESH_MS;
    entityCacheExpiryMs = DEFAULT_CACHE_EXPIRY_MS;
    entityMaxCacheSize = DEFAULT_MAX_CACHE_SIZE;
    enrichedEntityCacheRefreshMs = DEFAULT_CACHE_REFRESH_MS;
    enrichedEntityCacheExpiryMs = DEFAULT_CACHE_EXPIRY_MS;
    enrichedEntityMaxCacheSize = DEFAULT_MAX_CACHE_SIZE;
    entityIdsCacheRefreshMs = DEFAULT_CACHE_REFRESH_MS;
    entityIdsCacheExpiryMs = DEFAULT_CACHE_EXPIRY_MS;
    entityIdsMaxCacheSize = DEFAULT_MAX_CACHE_SIZE;
    isExpiredAfterAccess = false;
  }

  public long getEnrichedEntityCacheRefreshMs() {
    return enrichedEntityCacheRefreshMs;
  }

  public long getEnrichedEntityCacheExpiryMs() {
    return enrichedEntityCacheExpiryMs;
  }

  public long getEnrichedEntityMaxCacheSize() {
    return enrichedEntityMaxCacheSize;
  }

  public long getEntityCacheRefreshMs() {
    return entityCacheRefreshMs;
  }

  public long getEntityCacheExpiryMs() {
    return entityCacheExpiryMs;
  }

  public long getEntityMaxCacheSize() {
    return entityMaxCacheSize;
  }

  public long getEntityIdsCacheRefreshMs() {
    return entityIdsCacheRefreshMs;
  }

  public long getEntityIdsCacheExpiryMs() {
    return entityIdsCacheExpiryMs;
  }

  public long getEntityIdsMaxCacheSize() {
    return entityIdsMaxCacheSize;
  }

  public boolean isExpiredAfterAcesss() {
    return isExpiredAfterAccess;
  }
}
