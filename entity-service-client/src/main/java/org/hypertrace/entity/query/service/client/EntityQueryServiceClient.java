package org.hypertrace.entity.query.service.client;

import io.grpc.Channel;
import java.util.Iterator;
import java.util.Map;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc.EntityQueryServiceBlockingStub;
import org.hypertrace.entity.query.service.v1.EntityUpdateRequest;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;

public class EntityQueryServiceClient {

  private final EntityQueryServiceBlockingStub blockingStub;

  public EntityQueryServiceClient(Channel channel) {
    blockingStub = EntityQueryServiceGrpc.newBlockingStub(channel).withCallCredentials(
        RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  public Iterator<ResultSetChunk> execute(EntityQueryRequest request, Map<String, String> headers) {
    return GrpcClientRequestContextUtil
        .executeWithHeadersContext(headers, () -> blockingStub.execute(request));
  }

  public Iterator<ResultSetChunk> update(EntityUpdateRequest updateRequest,
      Map<String, String> headers) {
    return GrpcClientRequestContextUtil
        .executeWithHeadersContext(headers, () -> blockingStub.update(updateRequest));
  }
}
