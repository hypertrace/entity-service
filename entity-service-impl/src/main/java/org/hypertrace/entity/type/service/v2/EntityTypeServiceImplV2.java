package org.hypertrace.entity.type.service.v2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.service.constants.EntityCollectionConstants;
import org.hypertrace.entity.service.constants.EntityServiceConstants;
import org.hypertrace.entity.service.util.TenantUtils;
import org.hypertrace.entity.type.service.v2.EntityTypeServiceV2Grpc.EntityTypeServiceV2ImplBase;
import org.hypertrace.entity.type.service.v2.model.EntityTypeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityTypeServiceImplV2 extends EntityTypeServiceV2ImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(EntityTypeServiceImplV2.class);

  private final Collection entityTypeCollection;

  public EntityTypeServiceImplV2(Datastore datastore) {
    this.entityTypeCollection =
        datastore.getCollection(EntityCollectionConstants.ENTITY_TYPES_V2_COLLECTION);
  }

  @Override
  public void upsertEntityType(org.hypertrace.entity.type.service.v2.EntityType request,
      io.grpc.stub.StreamObserver<org.hypertrace.entity.type.service.v2.EntityType> responseObserver) {
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    try {
      // Set tenant id.
      EntityTypeDocument document = EntityTypeDocument.fromProto(request);
      document.setTenantId(tenantId.get());
      entityTypeCollection.upsert(new SingleValueKey(tenantId.get(), request.getName()), document);
      responseObserver.onNext(request);
      responseObserver.onCompleted();
    } catch (IOException e) {
      responseObserver.onError(new RuntimeException("Error upserting EntityType:" + request, e));
    }
  }

  @Override
  public void deleteEntityTypes(org.hypertrace.entity.type.service.v2.EntityTypeFilter request,
      io.grpc.stub.StreamObserver<org.hypertrace.entity.type.service.v2.Empty> responseObserver) {
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    try {
      // Bulk delete?
      request.getNameList()
          .forEach(e -> entityTypeCollection.delete(new SingleValueKey(tenantId.get(), e)));
      responseObserver.onNext(Empty.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception ex) {
      LOGGER.error("Error deleting entity types matching filter:{}", request);
      responseObserver.onError(ex);
    }
  }

  @Override
  public void queryEntityTypes(org.hypertrace.entity.type.service.v2.EntityTypeFilter request,
      io.grpc.stub.StreamObserver<org.hypertrace.entity.type.service.v2.EntityType> responseObserver) {
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    Iterator<Document> entityTypes =
        entityTypeCollection.search(transform(tenantId.get(), request));

    while (entityTypes.hasNext()) {
      String json = entityTypes.next().toJson();
      try {
        responseObserver.onNext(EntityTypeDocument.fromJson(json).toProto());
      } catch (JsonProcessingException e) {
        LOGGER.warn("Failed to parse the EntityType json: {}", json, e);
        responseObserver.onError(new RuntimeException("Error querying the entity types."));
      }
    }
    responseObserver.onCompleted();
  }

  private static Query transform(String tenantId, EntityTypeFilter filter) {
    Query docStoreQuery = new Query();
    List<Filter> filters = new ArrayList<>();
    filters.add(new Filter(Op.IN, EntityServiceConstants.TENANT_ID,
        TenantUtils.getTenantHierarchy(tenantId)));

    if (!filter.getNameList().isEmpty()) {
      filters.add(new Filter(Op.IN, EntityServiceConstants.NAME, filter.getNameList()));
    }

    if (filters.size() == 1) {
      docStoreQuery.setFilter(filters.get(0));
    } else {
      Filter f = new Filter();
      f.setOp(Filter.Op.AND);
      f.setChildFilters(filters.toArray(new Filter[]{}));
      docStoreQuery.setFilter(f);
    }
    return docStoreQuery;
  }
}
