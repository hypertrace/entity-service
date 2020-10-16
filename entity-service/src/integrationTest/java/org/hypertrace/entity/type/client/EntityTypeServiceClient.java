package org.hypertrace.entity.type.client;

import io.grpc.Channel;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.entity.type.service.v2.DeleteEntityTypesRequest;
import org.hypertrace.entity.type.service.v2.EntityType;
import org.hypertrace.entity.type.service.v2.EntityTypeServiceGrpc;
import org.hypertrace.entity.type.service.v2.EntityTypeServiceGrpc.EntityTypeServiceBlockingStub;
import org.hypertrace.entity.type.service.v2.QueryEntityTypesRequest;
import org.hypertrace.entity.type.service.v2.UpsertEntityTypeRequest;
import org.hypertrace.entity.type.service.v2.UpsertEntityTypeResponse;

public class EntityTypeServiceClient {
  private final EntityTypeServiceBlockingStub blockingStub;

  public EntityTypeServiceClient(Channel channel) {
    blockingStub = EntityTypeServiceGrpc.newBlockingStub(channel).withCallCredentials(
        RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  private <V> V execute(String tenantId, Callable<V> c) {
    return GrpcClientRequestContextUtil.executeInTenantContext(tenantId, c);
  }

  public UpsertEntityTypeResponse upsertEntityType(String tenantId, EntityType entityType) {
    return execute(tenantId, () -> blockingStub.upsertEntityType(
        UpsertEntityTypeRequest.newBuilder().setEntityType(entityType).build()));
  }

  public void deleteAllEntityTypes(String tenantId) {
    List<String> names =
        getAllEntityTypes(tenantId).stream().map(EntityType::getName).collect(Collectors.toList());
    if (!names.isEmpty()) {
      execute(tenantId, () -> blockingStub.deleteEntityTypes(
          DeleteEntityTypesRequest.newBuilder().addAllName(names).build()));
    }
  }

  public void deleteEntityTypes(String tenantId, List<String> entityTypes) {
    execute(tenantId, () -> blockingStub.deleteEntityTypes(
        DeleteEntityTypesRequest.newBuilder().addAllName(entityTypes).build()));
  }

  public List<EntityType> getAllEntityTypes(String tenantId) {
    return execute(tenantId,
        () -> blockingStub.queryEntityTypes(QueryEntityTypesRequest.newBuilder().build()))
        .getEntityTypeList();
  }

  public List<EntityType> queryEntityTypes(String tenantId, List<String> names) {
    return execute(tenantId, () -> blockingStub.queryEntityTypes(
        QueryEntityTypesRequest.newBuilder().addAllName(names).build())).getEntityTypeList();
  }
}

