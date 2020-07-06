package org.hypertrace.entity.data.service.client;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.hypertrace.entity.data.service.v1.ByTypeAndIdentifyingAttributes;
import org.hypertrace.entity.data.service.v1.EnrichedEntities;
import org.hypertrace.entity.data.service.v1.EnrichedEntity;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.EntityRelationship;
import org.hypertrace.entity.data.service.v1.EntityRelationships;
import org.hypertrace.entity.data.service.v1.Query;
import org.hypertrace.entity.service.client.config.EntityServiceClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EdsCacheClient implements EdsClient {

  private static final Logger LOG = LoggerFactory.getLogger(EdsCacheClient.class);
  private final LoadingCache<EdsCacheKey, Optional<EnrichedEntity>> enrichedEntityCache;
  private final LoadingCache<EdsCacheKey, Optional<Entity>> entityCache;

  private final EntityDataServiceClient client;

  EdsCacheClient(EntityServiceClientConfig entityServiceClientConfig) {
    this(new EntityDataServiceClient(entityServiceClientConfig));
  }

  public EdsCacheClient(EntityDataServiceClient client) {
    this.client = client;
    this.enrichedEntityCache =
        CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).maximumSize(1000)
            .build(new CacheLoader<>() {
              public Optional<EnrichedEntity> load(@Nonnull EdsCacheKey key) {
                return Optional
                    .ofNullable(client.getEnrichedEntityById(key.tenantId, key.entityId));
              }
            });

    this.entityCache =
        CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).maximumSize(1000)
            .build(new CacheLoader<>() {
              public Optional<Entity> load(@Nonnull EdsCacheKey key) {
                return Optional.ofNullable(client.getById(key.tenantId, key.entityId));
              }
            });
  }

  @Override
  public Entity upsert(Entity entity) {
    return client.upsert(entity);
  }

  @Override
  public Entity getByTypeAndIdentifyingAttributes(String tenantId,
      ByTypeAndIdentifyingAttributes byIdentifyingAttributes) {
    return client.getByTypeAndIdentifyingAttributes(tenantId, byIdentifyingAttributes);
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
      return entityCache.get(key).orElse(null);
    } catch (ExecutionException e) {
      LOG.error("Failed to fetch entity of tenantId: {}, entityId: {}",
          key.tenantId, key.entityId, e);
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
      return enrichedEntityCache.get(edsCacheKey).orElse(null);
    } catch (ExecutionException e) {
      LOG.error("Failed to fetch enriched entity of tenantId: {}, entityId: {}",
          edsCacheKey.tenantId, edsCacheKey.entityId, e);
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
