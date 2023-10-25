package org.hypertrace.entity.query.service.client;

import io.grpc.Channel;
import java.util.Iterator;
import java.util.Map;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.entity.query.service.v1.BulkEntityArrayAttributeUpdateRequest;
import org.hypertrace.entity.query.service.v1.BulkEntityArrayAttributeUpdateResponse;
import org.hypertrace.entity.query.service.v1.BulkEntityUpdateRequest;
import org.hypertrace.entity.query.service.v1.DeleteEntitiesRequest;
import org.hypertrace.entity.query.service.v1.DeleteEntitiesResponse;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc.EntityQueryServiceBlockingStub;
import org.hypertrace.entity.query.service.v1.EntityUpdateRequest;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.TotalEntitiesRequest;
import org.hypertrace.entity.query.service.v1.TotalEntitiesResponse;

/**
 * @deprecated Use gRPC stub clients instead
 */
@Deprecated(forRemoval = true)
public class EntityQueryServiceClient {

  private final EntityQueryServiceBlockingStub blockingStub;

  public EntityQueryServiceClient(Channel channel) {
    blockingStub =
        EntityQueryServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  public Iterator<ResultSetChunk> execute(EntityQueryRequest request, Map<String, String> headers) {
    return GrpcClientRequestContextUtil.executeWithHeadersContext(
        headers, () -> blockingStub.execute(request));
  }

  public Iterator<ResultSetChunk> update(
      EntityUpdateRequest updateRequest, Map<String, String> headers) {
    return GrpcClientRequestContextUtil.executeWithHeadersContext(
        headers, () -> blockingStub.update(updateRequest));
  }

  public BulkEntityArrayAttributeUpdateResponse bulkUpdateEntityArrayAttribute(
      BulkEntityArrayAttributeUpdateRequest request, Map<String, String> headers) {
    return GrpcClientRequestContextUtil.executeWithHeadersContext(
        headers, () -> blockingStub.bulkUpdateEntityArrayAttribute(request));
  }

  public TotalEntitiesResponse total(TotalEntitiesRequest request, Map<String, String> headers) {
    return GrpcClientRequestContextUtil.executeWithHeadersContext(
        headers, () -> blockingStub.total(request));
  }

  public Iterator<ResultSetChunk> bulkUpdate(
      BulkEntityUpdateRequest request, Map<String, String> headers) {
    return GrpcClientRequestContextUtil.executeWithHeadersContext(
        headers, () -> blockingStub.bulkUpdate(request));
  }

  public DeleteEntitiesResponse deleteEntities(
      DeleteEntitiesRequest request, Map<String, String> headers) {
    return GrpcClientRequestContextUtil.executeWithHeadersContext(
        headers, () -> blockingStub.deleteEntities(request));
  }
}
