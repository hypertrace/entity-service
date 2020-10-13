package org.hypertrace.entity.type.service.client;

import com.google.common.collect.Lists;
import io.grpc.Channel;
import java.util.List;
import java.util.concurrent.Callable;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.entity.type.service.v1.EntityRelationshipType;
import org.hypertrace.entity.type.service.v1.EntityRelationshipTypeFilter;
import org.hypertrace.entity.type.service.v1.EntityType;
import org.hypertrace.entity.type.service.v1.EntityTypeFilter;
import org.hypertrace.entity.type.service.v1.EntityTypeServiceGrpc;
import org.hypertrace.entity.type.service.v1.EntityTypeServiceGrpc.EntityTypeServiceBlockingStub;

/**
 * Client for all operations on EntityTypes stored in the Document Store
 */
public class EntityTypeServiceClient {

  private final EntityTypeServiceBlockingStub blockingStub;

  public EntityTypeServiceClient(Channel channel) {
    blockingStub = EntityTypeServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  private <V> V execute(String tenantId, Callable<V> c) {
    return GrpcClientRequestContextUtil.executeInTenantContext(tenantId, c);
  }

  public void upsertEntityType(String tenantId, EntityType entityType) {
    execute(tenantId, () -> blockingStub.upsertEntityType(entityType));
  }

  public void upsertEntityRelationshipType(String tenantId,
      EntityRelationshipType entityRelationshipType) {
    execute(tenantId, () -> blockingStub.upsertEntityRelationshipType(entityRelationshipType));
  }

  public void deleteEntityTypes(String tenantId, EntityTypeFilter entityTypeFilter) {
    execute(tenantId, () -> blockingStub.deleteEntityTypes(entityTypeFilter));
  }

  public void deleteEntityRelationshipTypes(String tenantId,
      EntityRelationshipTypeFilter entityRelationshipTypeFilter) {
    execute(tenantId,
        () -> blockingStub.deleteEntityRelationshipTypes(entityRelationshipTypeFilter));
  }

  public List<EntityType> getAllEntityTypes(String tenantId) {
    return Lists.newArrayList(execute(tenantId,
        () -> blockingStub.queryEntityTypes(EntityTypeFilter.newBuilder().build())));
  }

  public List<EntityType> queryEntityTypes(String tenantId, EntityTypeFilter entityTypeFilter) {
    return Lists
        .newArrayList(execute(tenantId, () -> blockingStub.queryEntityTypes(entityTypeFilter)));
  }

  public List<EntityRelationshipType> getAllEntityRelationshipTypes(String tenantId) {
    return Lists.newArrayList(execute(tenantId,
        () -> blockingStub.queryRelationshipTypes(
            EntityRelationshipTypeFilter.newBuilder().build())));
  }

  public List<EntityRelationshipType> queryRelationshipTypes(String tenantId,
      EntityRelationshipTypeFilter filter) {
    return Lists.newArrayList(execute(tenantId, () -> blockingStub.queryRelationshipTypes(filter)));
  }
}
