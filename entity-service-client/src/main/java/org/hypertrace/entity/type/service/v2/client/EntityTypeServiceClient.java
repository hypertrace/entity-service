package org.hypertrace.entity.type.service.v2.client;

import com.google.common.collect.Lists;
import io.grpc.Channel;
import java.util.List;
import java.util.concurrent.Callable;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.entity.type.service.v2.EntityType;
import org.hypertrace.entity.type.service.v2.EntityTypeFilter;
import org.hypertrace.entity.type.service.v2.EntityTypeServiceV2Grpc;
import org.hypertrace.entity.type.service.v2.EntityTypeServiceV2Grpc.EntityTypeServiceV2BlockingStub;

public class EntityTypeServiceClient {

  private final EntityTypeServiceV2BlockingStub blockingStub;

  public EntityTypeServiceClient(Channel channel) {
    blockingStub = EntityTypeServiceV2Grpc.newBlockingStub(channel).withCallCredentials(
        RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  private <V> V execute(String tenantId, Callable<V> c) {
    return GrpcClientRequestContextUtil.executeInTenantContext(tenantId, c);
  }

  public void upsertEntityType(String tenantId, EntityType entityType) {
    execute(tenantId, () -> blockingStub.upsertEntityType(entityType));
  }

  public void deleteEntityTypes(String tenantId, EntityTypeFilter entityTypeFilter) {
    execute(tenantId, () -> blockingStub.deleteEntityTypes(entityTypeFilter));
  }

  public List<EntityType> getAllEntityTypes(String tenantId) {
    return Lists.newArrayList(execute(tenantId,
        () -> blockingStub.queryEntityTypes(EntityTypeFilter.newBuilder().build())));
  }

  public List<EntityType> queryEntityTypes(String tenantId, EntityTypeFilter entityTypeFilter) {
    return Lists.newArrayList(execute(tenantId, () -> blockingStub.queryEntityTypes(entityTypeFilter)));
  }
}
