package org.hypertrace.entity.data.service.client;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.hypertrace.entity.data.service.client.exception.NotFoundException;
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
  private LoadingCache<EdsCacheKey, EnrichedEntity> enrichedEntityCache;
  private LoadingCache<EdsCacheKey, Entity> entityCache;
  private LoadingCache<EdsTypeAndIdAttributesCacheKey, String> entityIdsCache;

  private final EntityDataServiceClient client;

  public EdsCacheClient(EntityDataServiceClient client,
      EntityServiceClientCacheConfig cacheConfig) {
    this.client = client;
    initCache(cacheConfig);
  }

  private void initCache(EntityServiceClientCacheConfig cacheConfig) {
    this.enrichedEntityCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cacheConfig.getEnrichedEntityCacheExpiryMs(), TimeUnit.MILLISECONDS)
            .maximumSize(cacheConfig.getEnrichedEntityMaxCacheSize())
            .build(new CacheLoader<>() {
              public EnrichedEntity load(@Nonnull EdsCacheKey key) throws Exception {
                EnrichedEntity enrichedEntity = client
                    .getEnrichedEntityById(key.tenantId, key.entityId);
                if (enrichedEntity == null) {
                  throw new NotFoundException("Enriched entity not found");
                }
                return enrichedEntity;
              }
            });

    this.entityCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cacheConfig.getEntityCacheExpiryMs(), TimeUnit.MILLISECONDS)
            .maximumSize(cacheConfig.getEntityMaxCacheSize())
            .build(new CacheLoader<>() {
              public Entity load(@Nonnull EdsCacheKey key) throws Exception {
                Entity entity = client.getById(key.tenantId, key.entityId);
                if (entity == null) {
                  throw new NotFoundException("Entity not found");
                }
                return entity;
              }
            });

    this.entityIdsCache = CacheBuilder.newBuilder()
        .expireAfterWrite(cacheConfig.getEntityIdsCacheExpiryMs(), TimeUnit.MILLISECONDS)
        .maximumSize(cacheConfig.getEntityIdsMaxCacheSize())
        .build(new CacheLoader<>() {
          public String load(@Nonnull EdsTypeAndIdAttributesCacheKey key) throws Exception {
            Entity entity = client.getByTypeAndIdentifyingAttributes(key.tenantId,
                key.byTypeAndIdentifyingAttributes);
            if (entity == null) {
              throw new NotFoundException("Entity not found!!");
            }
            entityCache.put(new EdsCacheKey(entity.getTenantId(), entity.getEntityId()), entity);
            return entity.getEntityId();
          }
        });
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
  public Entity getByTypeAndIdentifyingAttributes(String tenantId,
      ByTypeAndIdentifyingAttributes byIdentifyingAttributes) {
    EdsTypeAndIdAttributesCacheKey key = new EdsTypeAndIdAttributesCacheKey(tenantId,
        byIdentifyingAttributes);
    try {
      return getById(tenantId, entityIdsCache.get(key));
    } catch (ExecutionException e) {
      LOG.debug("Failed to fetch entity of tenantId: {}, entityId: {}",
              key.tenantId, key.byTypeAndIdentifyingAttributes);
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
      LOG.debug("Failed to fetch entity of tenantId: {}, entityId: {}",
              key.tenantId, key.entityId);
      return null;
    }
  }

  @Override
  public List<Entity> query(String tenantId, Query query) {
    return client.query(tenantId, query);
  }

  @Override
  public Iterator<EntityRelationship> getRelationships(String tenantId,
      Set<String> relationshipTypes,
      Set<String> fromIds, Set<String> toIds) {
    return client.getRelationships(tenantId, relationshipTypes, fromIds, toIds);
  }

  @Override
  public EnrichedEntity getEnrichedEntityById(String tenantId, String entityId) {
    EdsCacheKey edsCacheKey = new EdsCacheKey(tenantId, entityId);
    try {
      return enrichedEntityCache.get(edsCacheKey);
    } catch (ExecutionException e) {
      LOG.debug("Failed to fetch enriched entity of tenantId: {}, entityId: {}",
              edsCacheKey.tenantId, edsCacheKey.entityId);
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
}
