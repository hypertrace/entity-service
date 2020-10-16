package org.hypertrace.entity.type.service.v2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Strings;
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
import org.hypertrace.entity.type.service.v2.EntityTypeServiceGrpc.EntityTypeServiceImplBase;
import org.hypertrace.entity.type.service.v2.model.EntityTypeDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityTypeServiceImpl extends EntityTypeServiceImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(EntityTypeServiceImpl.class);

  private final Collection entityTypeCollection;

  public EntityTypeServiceImpl(Datastore datastore) {
    this.entityTypeCollection =
        datastore.getCollection(EntityCollectionConstants.ENTITY_TYPES_V2_COLLECTION);
  }

  @Override
  public void upsertEntityType(org.hypertrace.entity.type.service.v2.UpsertEntityTypeRequest request,
      io.grpc.stub.StreamObserver<org.hypertrace.entity.type.service.v2.UpsertEntityTypeResponse> responseObserver) {
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    // validate the request.
    if (!isValidUpsert(request.getEntityType())) {
      responseObserver.onError(new ServiceException("Invalid upsert request."));
      return;
    }

    try {
      // Set tenant id.
      EntityTypeDocument document = EntityTypeDocument.fromProto(tenantId.get(), request.getEntityType());
      entityTypeCollection.upsert(new SingleValueKey(tenantId.get(), request.getEntityType().getName()), document);

      // Query the entity type again and return that.
      Iterator<Document> entityTypes =
          entityTypeCollection.search(transform(tenantId.get(), List.of(request.getEntityType().getName())));

      if (entityTypes.hasNext()) {
        String json = entityTypes.next().toJson();
          responseObserver.onNext(UpsertEntityTypeResponse.newBuilder()
              .setEntityType(EntityTypeDocument.fromJson(json).toProto()).build());
        responseObserver.onCompleted();
      } else {
        // This could happen if the upsert has failed to make the doc store.
        // Return an error for now.
        responseObserver.onError(new RuntimeException("Error upserting EntityType:" + request));
      }
    } catch (IOException e) {
      responseObserver.onError(new RuntimeException("Error upserting EntityType:" + request, e));
    }
  }

  private boolean isValidUpsert(EntityType entityType) {
    return !Strings.isNullOrEmpty(entityType.getName()) &&
        !Strings.isNullOrEmpty(entityType.getAttributeScope()) &&
        !Strings.isNullOrEmpty(entityType.getIdAttributeKey()) &&
        !Strings.isNullOrEmpty(entityType.getNameAttributeKey());
  }

  @Override
  public void deleteEntityTypes(org.hypertrace.entity.type.service.v2.DeleteEntityTypesRequest request,
      io.grpc.stub.StreamObserver<org.hypertrace.entity.type.service.v2.DeleteEntityTypesResponse> responseObserver) {
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    if (request.getNameCount() == 0) {
      responseObserver.onError(new RuntimeException("Invalid delete request."));
      return;
    }

    request.getNameList()
        .forEach(e -> entityTypeCollection.delete(new SingleValueKey(tenantId.get(), e)));
    responseObserver.onNext(DeleteEntityTypesResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void queryEntityTypes(org.hypertrace.entity.type.service.v2.QueryEntityTypesRequest request,
      io.grpc.stub.StreamObserver<org.hypertrace.entity.type.service.v2.QueryEntityTypesResponse> responseObserver) {
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    Iterator<Document> entityTypes =
        entityTypeCollection.search(transform(tenantId.get(), request.getNameList()));

    List<EntityType> entityTypeList = new ArrayList<>();
    while (entityTypes.hasNext()) {
      String json = entityTypes.next().toJson();
      try {
        entityTypeList.add(EntityTypeDocument.fromJson(json).toProto());
      } catch (JsonProcessingException e) {
        LOGGER.warn("Failed to parse the EntityType json: {}", json, e);
        responseObserver.onError(new RuntimeException("Error querying the entity types."));
        return;
      }
    }
    responseObserver.onNext(
        QueryEntityTypesResponse.newBuilder().addAllEntityType(entityTypeList).build());
    responseObserver.onCompleted();
  }

  private static Query transform(String tenantId, List<String> entityTypeNames) {
    Query docStoreQuery = new Query();
    List<Filter> filters = new ArrayList<>();
    filters.add(new Filter(Op.IN, EntityServiceConstants.TENANT_ID,
        TenantUtils.getTenantHierarchy(tenantId)));

    if (!entityTypeNames.isEmpty()) {
      filters.add(new Filter(Op.IN, EntityServiceConstants.NAME, entityTypeNames));
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
