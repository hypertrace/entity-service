package org.hypertrace.entity.data.service.client;

import static org.hypertrace.entity.service.constants.EntityConstants.attributeMapPathFor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.grpc.Channel;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.entity.data.service.v1.AttributeFilter;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.ByIdRequest;
import org.hypertrace.entity.data.service.v1.ByTypeAndIdentifyingAttributes;
import org.hypertrace.entity.data.service.v1.EnrichedEntities;
import org.hypertrace.entity.data.service.v1.EnrichedEntity;
import org.hypertrace.entity.data.service.v1.Entities;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc.EntityDataServiceBlockingStub;
import org.hypertrace.entity.data.service.v1.EntityRelationship;
import org.hypertrace.entity.data.service.v1.EntityRelationships;
import org.hypertrace.entity.data.service.v1.Operator;
import org.hypertrace.entity.data.service.v1.Query;
import org.hypertrace.entity.data.service.v1.RelationshipsQuery;
import org.hypertrace.entity.data.service.v1.RelationshipsQuery.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for all CRUD and Query operations on Entities stored in the Document Store
 */
public class EntityDataServiceClient implements EdsClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(EntityDataServiceClient.class);
  private final EntityDataServiceBlockingStub blockingStub;

  public EntityDataServiceClient(Channel channel) {
    blockingStub = EntityDataServiceGrpc.newBlockingStub(channel).withCallCredentials(
        RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  private <V> V execute(String tenantId, Callable<V> c) {
    return GrpcClientRequestContextUtil.executeInTenantContext(tenantId, c);
  }

  @Nullable
  @Override
  public Entity upsert(Entity entity) {
    Entity result = execute(entity.getTenantId(), () -> blockingStub.upsert(entity));
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Upserted entity: {}", result);
    }
    return result.equals(Entity.getDefaultInstance()) ? null : result;
  }

  @Override
  public Iterator<Entity> getAndBulkUpsert(String tenantId, Collection<Entity> entities) {
    return execute(tenantId, () -> blockingStub.getAndUpsertEntities(Entities.newBuilder().addAllEntity(entities).build()));
  }

  public void bulkUpsert(String tenantId, java.util.Collection<Entity> entities) {
    execute(tenantId,
        () -> blockingStub.upsertEntities(Entities.newBuilder().addAllEntity(entities).build()));
  }

  public void delete(String tenantId, String entityId) {
    execute(tenantId,
        () -> blockingStub.delete(ByIdRequest.newBuilder().setEntityId(entityId).build()));
  }

  @Nullable
  @Override
  public Entity getByTypeAndIdentifyingAttributes(String tenantId,
      ByTypeAndIdentifyingAttributes byIdentifyingAttributes) {
    Entity entity = execute(tenantId,
        () -> blockingStub.getByTypeAndIdentifyingProperties(byIdentifyingAttributes));
    // Handle this here, so that callers can just do a null check
    return entity.equals(Entity.getDefaultInstance()) ? null : entity;
  }

  @Override
  public List<Entity> getEntitiesByType(String tenantId, String entityType) {
    Query query = Query.newBuilder().setEntityType(entityType).build();
    return query(tenantId, query);
  }

  @Override
  public List<Entity> getEntitiesByName(String tenantId, String entityType, String entityName) {
    Query query = Query.newBuilder().setEntityType(entityType).setEntityName(entityName).build();
    return query(tenantId, query);
  }

  @Nullable
  public Entity getById(String tenantId, String entityId) {
    Entity entity = execute(tenantId,
        () -> blockingStub.getById(ByIdRequest.newBuilder().setEntityId(entityId).build()));

    // Handle this here, so that callers can just do a null check
    return entity.equals(Entity.getDefaultInstance()) ? null : entity;
  }

  @VisibleForTesting
  public List<Entity> query(String tenantId, Query query) {
    return execute(tenantId, () -> Lists.newArrayList(blockingStub.query(query)));
  }

  public List<Entity> getEntitiesWithGivenAttribute(
      String tenantId, String entityType, String attributeKey, AttributeValue attributeValue) {
    Query query = Query.newBuilder()
        .setEntityType(entityType)
        .setFilter(AttributeFilter.newBuilder()
            .setName(attributeMapPathFor(attributeKey))
            .setOperator(Operator.EQ)
            .setAttributeValue(attributeValue)
            .build())
        .build();

    return query(tenantId, query);
  }

  @Override
  public void upsertRelationships(String tenantId, EntityRelationships relationships) {
    execute(tenantId, () -> blockingStub.upsertRelationships(relationships));
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Upserted entityRelationships: {}", relationships);
    }
  }

  @Override
  public Iterator<EntityRelationship> getRelationships(
      String tenantId, @Nullable Set<String> entityRelationshipTypes,
      @Nullable Set<String> fromEntityIds, @Nullable Set<String> toEntityIds) {
    Builder builder = RelationshipsQuery.newBuilder();

    if (entityRelationshipTypes != null && !entityRelationshipTypes.isEmpty()) {
      builder.addAllEntityRelationship(entityRelationshipTypes);
    }
    if (fromEntityIds != null && !fromEntityIds.isEmpty()) {
      builder.addAllFromEntityId(fromEntityIds);
    }
    if (toEntityIds != null && !toEntityIds.isEmpty()) {
      builder.addAllToEntityId(toEntityIds);
    }
    return execute(tenantId, () -> blockingStub.getRelationships(builder.build()));
  }

  @Override
  public EnrichedEntity upsertEnrichedEntity(EnrichedEntity entity) {
    EnrichedEntity result =
        execute(entity.getTenantId(), () -> blockingStub.upsertEnrichedEntity(entity));
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Upserted entity: {}", result);
    }
    return result;
  }

  /**
   * Bulk upsert the given enriched entities. Please note that there could be a failure in halfway
   * while upserting the given enriched entities, in which case only some entities would have been
   * made to the service. Client should decide whether to retry or not in those cases.
   *
   * @param tenantId Tenant id for the enriched entities. All of them should belong to the same
   *                 tenant.
   * @param entities Enriched entities to be upserted.
   */
  @Override
  public void upsertEnrichedEntities(String tenantId, EnrichedEntities entities) {
    execute(tenantId, () -> blockingStub.upsertEnrichedEntities(entities));
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Upserted entities: {}", entities);
    }
  }

  @Nullable
  @Override
  public EnrichedEntity getEnrichedEntityById(String tenantId, String entityId) {
    ByIdRequest byIdRequest = ByIdRequest.newBuilder().setEntityId(entityId).build();
    EnrichedEntity entity = execute(tenantId,
        () -> blockingStub.getEnrichedEntityById(byIdRequest));

    // Handle this here, so that callers can just do a null check
    return entity.equals(EnrichedEntity.getDefaultInstance()) ? null : entity;
  }
}
