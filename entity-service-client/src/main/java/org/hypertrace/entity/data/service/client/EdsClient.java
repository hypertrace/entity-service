package org.hypertrace.entity.data.service.client;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.hypertrace.entity.data.service.v1.ByTypeAndIdentifyingAttributes;
import org.hypertrace.entity.data.service.v1.EnrichedEntities;
import org.hypertrace.entity.data.service.v1.EnrichedEntity;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.EntityRelationship;
import org.hypertrace.entity.data.service.v1.EntityRelationships;
import org.hypertrace.entity.data.service.v1.Query;

/**
 * Entity Data Service Client interface.
 */
public interface EdsClient {

  Entity upsert(Entity entity);

  Iterator<Entity> getAndBulkUpsert(String tenantId, Collection<Entity> entities);

  Entity getByTypeAndIdentifyingAttributes(String tenantId,
      ByTypeAndIdentifyingAttributes byIdentifyingAttributes);

  List<Entity> getEntitiesByType(String tenantId, String entityType);

  List<Entity> getEntitiesByName(String tenantId, String entityType, String entityName);

  Entity getById(String tenantId, String entityId);

  List<Entity> query(String tenantId, Query query);

  /**
   * Upsert relationships in batch.
   *
   * @param tenantId      TenantId for the request
   * @param relationships entity relationships to create
   */
  void upsertRelationships(String tenantId, EntityRelationships relationships);

  Iterator<EntityRelationship> getRelationships(String tenantId, Set<String> relationshipTypes,
      Set<String> fromIds, Set<String> toIds);

  EnrichedEntity getEnrichedEntityById(String tenantId, String entityId);

  EnrichedEntity upsertEnrichedEntity(EnrichedEntity enrichedEntity);

  void upsertEnrichedEntities(String tenantId, EnrichedEntities enrichedEntities);
}
