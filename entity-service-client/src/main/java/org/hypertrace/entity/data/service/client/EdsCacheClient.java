package org.hypertrace.entity.data.service.client;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.entity.change.event.v1.EntityChangeEventKey;
import org.hypertrace.entity.change.event.v1.EntityChangeEventValue;
import org.hypertrace.entity.data.service.client.exception.NotFoundException;
import org.hypertrace.entity.data.service.v1.ByIdRequest;
import org.hypertrace.entity.data.service.v1.ByTypeAndIdentifyingAttributes;
import org.hypertrace.entity.data.service.v1.EnrichedEntities;
import org.hypertrace.entity.data.service.v1.EnrichedEntity;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.EntityRelationship;
import org.hypertrace.entity.data.service.v1.EntityRelationships;
import org.hypertrace.entity.data.service.v1.Query;
import org.hypertrace.entity.service.client.config.EntityServiceClientCacheConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EdsCacheClient implements EdsClient {

  private static final Logger LOG = LoggerFactory.getLogger(EdsCacheClient.class);
  private final EntityDataServiceClient client;
  private LoadingCache<EdsCacheKey, EnrichedEntity> enrichedEntityCache;
  private LoadingCache<EdsCacheKey, Entity> entityCache;
  private LoadingCache<EdsTypeAndIdAttributesCacheKey, String> entityIdsCache;

  public EdsCacheClient(
      EntityDataServiceClient client, EntityServiceClientCacheConfig cacheConfig) {
    this(client, cacheConfig, Runnable::run);
  }

  public EdsCacheClient(
      EntityDataServiceClient client,
      EntityServiceClientCacheConfig cacheConfig,
      Executor cacheLoaderExecutor) {
    this.client = client;
    initCache(cacheConfig, cacheLoaderExecutor);
  }

  private void initCache(EntityServiceClientCacheConfig cacheConfig, Executor executor) {
    this.enrichedEntityCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(cacheConfig.getEnrichedEntityCacheRefreshMs(), TimeUnit.MILLISECONDS)
            .expireAfterWrite(cacheConfig.getEnrichedEntityCacheExpiryMs(), TimeUnit.MILLISECONDS)
            .maximumSize(cacheConfig.getEnrichedEntityMaxCacheSize())
            .recordStats()
            .build(
                CacheLoader.asyncReloading(
                    new CacheLoader<>() {
                      public EnrichedEntity load(@Nonnull EdsCacheKey key) throws Exception {
                        EnrichedEntity enrichedEntity =
                            client.getEnrichedEntityById(key.tenantId, key.entityId);
                        if (enrichedEntity == null) {
                          throw new NotFoundException("Enriched entity not found");
                        }
                        return enrichedEntity;
                      }
                    },
                    executor));

    this.entityCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(cacheConfig.getEntityCacheRefreshMs(), TimeUnit.MILLISECONDS)
            .expireAfterWrite(cacheConfig.getEntityCacheExpiryMs(), TimeUnit.MILLISECONDS)
            .maximumSize(cacheConfig.getEntityMaxCacheSize())
            .recordStats()
            .build(
                CacheLoader.asyncReloading(
                    new CacheLoader<>() {
                      public Entity load(@Nonnull EdsCacheKey key) throws Exception {
                        Entity entity =
                            client.getById(
                                key.tenantId,
                                ByIdRequest.newBuilder()
                                    .setEntityType(key.entityType)
                                    .setEntityId(key.entityId)
                                    .build());
                        if (entity == null) {
                          throw new NotFoundException("Entity not found");
                        }
                        return entity;
                      }
                    },
                    executor));

    this.entityIdsCache =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(cacheConfig.getEntityIdsCacheRefreshMs(), TimeUnit.MILLISECONDS)
            .expireAfterWrite(cacheConfig.getEntityIdsCacheExpiryMs(), TimeUnit.MILLISECONDS)
            .maximumSize(cacheConfig.getEntityIdsMaxCacheSize())
            .recordStats()
            .build(
                CacheLoader.asyncReloading(
                    new CacheLoader<>() {
                      public String load(@Nonnull EdsTypeAndIdAttributesCacheKey key)
                          throws Exception {
                        Entity entity =
                            client.getByTypeAndIdentifyingAttributes(
                                key.tenantId, key.byTypeAndIdentifyingAttributes);
                        if (entity == null) {
                          throw new NotFoundException("Entity not found!!");
                        }
                        entityCache.put(
                            new EdsCacheKey(entity.getTenantId(), entity.getEntityId()), entity);
                        return entity.getEntityId();
                      }
                    },
                    executor));
    PlatformMetricsRegistry.registerCache(
        this.getClass().getName() + ".enrichedEntityCache",
        enrichedEntityCache,
        Collections.emptyMap());
    PlatformMetricsRegistry.registerCache(
        this.getClass().getName() + ".entityCache", entityCache, Collections.emptyMap());
    PlatformMetricsRegistry.registerCache(
        this.getClass().getName() + ".entityIdsCache", entityIdsCache, Collections.emptyMap());
  }

  @Override
  public Entity upsert(Entity entity) {
    return client.upsert(entity);
  }

  @Override
  public Iterator<Entity> getAndBulkUpsert(String tenantId, Collection<Entity> entities) {
    return client.getAndBulkUpsert(tenantId, entities);
  }

  @Override
  public Entity getByTypeAndIdentifyingAttributes(
      String tenantId, ByTypeAndIdentifyingAttributes byIdentifyingAttributes) {
    EdsTypeAndIdAttributesCacheKey key =
        new EdsTypeAndIdAttributesCacheKey(tenantId, byIdentifyingAttributes);
    try {
      return getById(tenantId, entityIdsCache.get(key));
    } catch (ExecutionException e) {
      LOG.debug(
          "Failed to fetch entity of tenantId: {}, entityId: {}",
          key.tenantId,
          key.byTypeAndIdentifyingAttributes);
      return null;
    }
  }

  @Override
  public List<Entity> getEntitiesByType(String tenantId, String entityType) {
    return client.getEntitiesByType(tenantId, entityType);
  }

  @Override
  public List<Entity> getEntitiesByName(String tenantId, String entityType, String entityName) {
    return client.getEntitiesByName(tenantId, entityType, entityName);
  }

  @Override
  public Entity getById(String tenantId, String entityId) {
    EdsCacheKey key = new EdsCacheKey(tenantId, entityId);
    try {
      return entityCache.get(key);
    } catch (ExecutionException e) {
      LOG.debug("Failed to fetch entity of tenantId: {}, entityId: {}", key.tenantId, key.entityId);
      return null;
    }
  }

  @Override
  public Entity getById(String tenantId, ByIdRequest byIdRequest) {
    EdsCacheKey key =
        new EdsCacheKey(tenantId, byIdRequest.getEntityId(), byIdRequest.getEntityType());
    try {
      return entityCache.get(key);
    } catch (ExecutionException e) {
      LOG.debug("Failed to fetch entity of tenantId: {}, entityId: {}", key.tenantId, key.entityId);
      return null;
    }
  }

  @Override
  public List<Entity> query(String tenantId, Query query) {
    return client.query(tenantId, query);
  }

  @Override
  public Iterator<EntityRelationship> getRelationships(
      String tenantId, Set<String> relationshipTypes, Set<String> fromIds, Set<String> toIds) {
    return client.getRelationships(tenantId, relationshipTypes, fromIds, toIds);
  }

  @Override
  public EnrichedEntity getEnrichedEntityById(String tenantId, String entityId) {
    EdsCacheKey edsCacheKey = new EdsCacheKey(tenantId, entityId);
    try {
      return enrichedEntityCache.get(edsCacheKey);
    } catch (ExecutionException e) {
      LOG.debug(
          "Failed to fetch enriched entity of tenantId: {}, entityId: {}",
          edsCacheKey.tenantId,
          edsCacheKey.entityId);
      return null;
    }
  }

  @Override
  public EnrichedEntity upsertEnrichedEntity(EnrichedEntity enrichedEntity) {
    return client.upsertEnrichedEntity(enrichedEntity);
  }

  @Override
  public void upsertEnrichedEntities(String tenantId, EnrichedEntities enrichedEntities) {
    client.upsertEnrichedEntities(tenantId, enrichedEntities);
  }

  @Override
  public void upsertRelationships(String tenantId, EntityRelationships relationships) {
    client.upsertRelationships(tenantId, relationships);
  }

  public void updateBasedOnChangeEvent(
      EntityChangeEventKey entityChangeEventKey, EntityChangeEventValue entityChangeEventValue) {
    LOG.debug("Entity change event is {}, {} ", entityChangeEventKey, entityChangeEventValue);

    switch (entityChangeEventValue.getEventCase()) {
      case CREATE_EVENT:
        // ignore create events, don't populate caches if not necessary
        break;
      case UPDATE_EVENT:
        updateCacheValues(
            entityChangeEventKey, entityChangeEventValue.getUpdateEvent().getLatestEntity());
        break;
      case DELETE_EVENT:
        getEntityCacheKeys(entityChangeEventKey)
            .forEach(cacheKey -> entityCache.invalidate(cacheKey));
        entityIdsCache.invalidate(
            getIdsCacheKey(
                entityChangeEventKey, entityChangeEventValue.getDeleteEvent().getDeletedEntity()));
        break;
      default:
        LOG.warn(
            "Entity change event value has invalid event type -> {}",
            entityChangeEventValue.getEventCase());
    }
  }

  private void updateCacheValues(EntityChangeEventKey entityChangeEventKey, Entity entity) {
    getEntityCacheKeys(entityChangeEventKey)
        .forEach(
            cacheKey -> entityCache.asMap().computeIfPresent(cacheKey, (key, oldEntity) -> entity));
    entityIdsCache
        .asMap()
        .computeIfPresent(
            getIdsCacheKey(entityChangeEventKey, entity),
            (cacheKey, oldEntity) -> entity.getEntityId());
  }

  private Iterable<EdsCacheKey> getEntityCacheKeys(EntityChangeEventKey entityChangeEventKey) {
    return List.of(
        getEntityCacheKeyWithoutEntityType(entityChangeEventKey),
        getEntityCacheKeyWithEntityType(entityChangeEventKey));
  }

  private EdsCacheKey getEntityCacheKeyWithoutEntityType(
      // used by - `getById(String tenantId, String entityId)`
      EntityChangeEventKey entityChangeEventKey) {
    return new EdsCacheKey(entityChangeEventKey.getTenantId(), entityChangeEventKey.getEntityId());
  }

  private EdsCacheKey getEntityCacheKeyWithEntityType(EntityChangeEventKey entityChangeEventKey) {
    // used by `getById(String tenantId, ByIdRequest byIdRequest)`
    return new EdsCacheKey(
        entityChangeEventKey.getTenantId(),
        entityChangeEventKey.getEntityId(),
        entityChangeEventKey.getEntityType());
  }

  private EdsTypeAndIdAttributesCacheKey getIdsCacheKey(
      EntityChangeEventKey entityChangeEventKey, Entity entity) {
    return new EdsTypeAndIdAttributesCacheKey(
        entityChangeEventKey.getTenantId(),
        ByTypeAndIdentifyingAttributes.newBuilder()
            .setEntityType(entity.getEntityType())
            .putAllIdentifyingAttributes(entity.getIdentifyingAttributesMap())
            .build());
  }
}
