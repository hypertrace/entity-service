package org.hypertrace.entity.type.service;

import static org.hypertrace.entity.service.constants.EntityCollectionConstants.ENTITY_TYPES_COLLECTION;
import static org.hypertrace.entity.service.constants.EntityCollectionConstants.ENTITY_TYPE_RELATIONS_COLLECTION;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ServiceException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.service.constants.EntityServiceConstants;
import org.hypertrace.entity.service.util.DocStoreConverter;
import org.hypertrace.entity.service.util.DocStoreJsonFormat;
import org.hypertrace.entity.service.util.StringUtils;
import org.hypertrace.entity.service.util.TenantUtils;
import org.hypertrace.entity.type.service.v1.Empty;
import org.hypertrace.entity.type.service.v1.EntityRelationshipType;
import org.hypertrace.entity.type.service.v1.EntityRelationshipTypeFilter;
import org.hypertrace.entity.type.service.v1.EntityType;
import org.hypertrace.entity.type.service.v1.EntityTypeFilter;
import org.hypertrace.entity.type.service.v1.EntityTypeServiceGrpc.EntityTypeServiceImplBase;
import org.hypertrace.entity.type.service.v1.MultiplicityKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityTypeServiceImpl extends EntityTypeServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(EntityTypeServiceImpl.class);
  private static final DocStoreJsonFormat.Printer PROTO_PRINTER = DocStoreJsonFormat.printer();
  private static final DocStoreJsonFormat.Parser PARSER =
      DocStoreJsonFormat.parser().ignoringUnknownFields();

  private static final String NAME = "name";
  private static final String FROM_ENTITY_TYPE = "fromEntityType";
  private static final String TO_ENTITY_TYPE = "toEntityType";
  private static final String MULTIPLICITY_KIND = "multiplicityKind";

  private final Collection entityTypeCol;
  private final Collection entityTypeRelationsCol;

  public EntityTypeServiceImpl(Datastore datastore) {
    this.entityTypeCol = datastore.getCollection(ENTITY_TYPES_COLLECTION);
    this.entityTypeRelationsCol =
        datastore.getCollection(ENTITY_TYPE_RELATIONS_COLLECTION);
  }

  @Override
  public void upsertEntityType(EntityType request, StreamObserver<EntityType> responseObserver) {
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    try {
      // TODO: Since we currently use the same object that's received in the request while
      //  persisting in the doc store, we need to add tenantId explicitly here.
      //  We need to split these objects later.
      EntityType newRequest = EntityType.newBuilder(request).setTenantId(tenantId.get()).build();

      entityTypeCol.upsert(new SingleValueKey(tenantId.get(), request.getName()),
          new JSONDocument(PROTO_PRINTER.print(newRequest)));
      responseObserver.onNext(newRequest);
      responseObserver.onCompleted();
    } catch (IOException ioe) {
      responseObserver.onError(new RuntimeException("Error upserting EntityType:" + request, ioe));
    }
  }

  @Override
  public void upsertEntityRelationshipType(EntityRelationshipType request,
      StreamObserver<EntityRelationshipType> responseObserver) {
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    try {
      // TODO: Since we currently use the same object that's received in the request while
      //  persisting in the doc store, we need to add tenantId explicitly here.
      //  We need to split these objects later.
      EntityRelationshipType newRequest = EntityRelationshipType.newBuilder(request)
          .setTenantId(tenantId.get())
          .build();

      entityTypeRelationsCol.upsert(new SingleValueKey(tenantId.get(), request.getName()),
          new JSONDocument(PROTO_PRINTER.print(newRequest)));
      responseObserver.onNext(newRequest);
      responseObserver.onCompleted();
    } catch (IOException ioe) {
      responseObserver.onError(new RuntimeException("Error upserting EntityType:" + request, ioe));
    }
  }

  @Override
  public void deleteEntityTypes(EntityTypeFilter request, StreamObserver<Empty> responseObserver) {
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    try {
      Iterator<Document> documents = entityTypeCol
          .search(transform(tenantId.get(), request, false));
      StreamSupport
          .stream(Spliterators.spliteratorUnknownSize(documents, 0), false)
          .map(document -> {
            try {
              EntityType.Builder builder = EntityType.newBuilder();
              PARSER.merge(document.toJson(), builder);
              return builder;
            } catch (InvalidProtocolBufferException e) {
              LOG.warn(String.format("Error processing entityType: %s", document.toJson()), e);
              return null;
            }
          })
          .filter(Objects::nonNull)
          .forEach(entityTypeBuilder -> entityTypeCol.delete(
              new SingleValueKey(tenantId.get(), entityTypeBuilder.getName())));
      responseObserver.onNext(Empty.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception ex) {
      LOG.error("Error deleting entity types matching filter:{}", request);
      responseObserver.onError(ex);
    }
  }

  @Override
  public void deleteEntityRelationshipTypes(
      EntityRelationshipTypeFilter request, StreamObserver<Empty> responseObserver) {
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    try {
      Iterator<Document> documents = entityTypeRelationsCol
          .search(transform(tenantId.get(), request, false));
      StreamSupport
          .stream(Spliterators.spliteratorUnknownSize(documents, 0), false)
          .map(document -> {
            try {
              EntityRelationshipType.Builder builder = EntityRelationshipType.newBuilder();
              PARSER.merge(document.toJson(), builder);
              return builder;
            } catch (InvalidProtocolBufferException e) {
              LOG.warn(String.format(
                  "Error processing entityRelationshipType: %s", document.toJson()), e);
              return null;
            }
          })
          .filter(Objects::nonNull)
          .forEach(entityRelationshipTypeBuilder ->
              entityTypeRelationsCol.delete(
                  new SingleValueKey(tenantId.get(), entityRelationshipTypeBuilder.getName())));
      responseObserver.onNext(Empty.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception ex) {
      LOG.error("Error deleting entity relationship types matching filter:{}", request);
      responseObserver.onError(ex);
    }
  }

  @Override
  public void queryEntityTypes(EntityTypeFilter request,
      StreamObserver<EntityType> responseObserver) {
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    Iterator<Document> entityTypes =
        entityTypeCol.search(transform(tenantId.get(), request, true));

    while (entityTypes.hasNext()) {
      Document entityType = entityTypes.next();

      EntityType.Builder builder = EntityType.newBuilder();
      try {
        PARSER.merge(entityType.toJson(), builder);

        // Populate the tenant id field with the tenant id that's received for backward
        // compatibility.
        builder.setTenantId(tenantId.get());
      } catch (InvalidProtocolBufferException e) {
        LOG.error("Error processing entityType: {}", entityType.toJson(), e);
        responseObserver.onError(e);
        return;
      }
      responseObserver.onNext(builder.build());
    }
    responseObserver.onCompleted();
  }

  @Override
  public void queryRelationshipTypes(EntityRelationshipTypeFilter request,
      StreamObserver<EntityRelationshipType> responseObserver) {
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    searchByQueryAndStreamResponse(responseObserver,
        transform(tenantId.get(), request, true), tenantId.get());
  }

  private void searchByQueryAndStreamResponse(
      StreamObserver<EntityRelationshipType> responseObserver,
      Query query, String tenantId) {
    Iterator<Document> entityTypeRels = entityTypeRelationsCol.search(query);

    while (entityTypeRels.hasNext()) {
      Document relation = entityTypeRels.next();

      EntityRelationshipType.Builder builder = EntityRelationshipType.newBuilder();
      try {
        PARSER.merge(relation.toJson(), builder);

        // Populate the tenant id field with the tenant id that's received for backward
        // compatibility.
        builder.setTenantId(tenantId);

      } catch (InvalidProtocolBufferException e) {
        LOG.error("Error processing entityType: {}", relation.toJson(), e);
        responseObserver.onError(e);
        return;
      }
      responseObserver.onNext(builder.build());
    }
    responseObserver.onCompleted();
  }

  private static Query transform(String tenantId, EntityTypeFilter filter,
      boolean useTenantHierarchy) {
    Query docStoreQuery = new Query();
    List<Filter> filters = new ArrayList<>();
    filters.add(getTenantIdFilter(tenantId, useTenantHierarchy));

    if (!filter.getNameList().isEmpty()) {
      filters.add(new Filter(Op.IN, NAME, filter.getNameList()));
    }

    if (!filters.isEmpty()) {
      if (filters.size() == 1) {
        docStoreQuery.setFilter(filters.get(0));
      } else {
        Filter f = new Filter();
        f.setOp(Filter.Op.AND);
        f.setChildFilters(filters.toArray(new Filter[]{}));
        docStoreQuery.setFilter(f);
      }
    }
    return docStoreQuery;
  }

  private static Filter getTenantIdFilter(String tenantId, boolean useTenantHierarchy) {
    if (useTenantHierarchy) {
      return new Filter(Op.IN, EntityServiceConstants.TENANT_ID,
          TenantUtils.getTenantHierarchy(tenantId));
    }
    return DocStoreConverter.getTenantIdEqFilter(tenantId);
  }

  private static Query transform(String tenantId, EntityRelationshipTypeFilter filter,
      boolean useTenantHierarchy) {
    Query docStoreQuery = new Query();
    List<Filter> filters = new ArrayList<>();
    if (StringUtils.isNotEmpty(filter.getFromEntityType())) {
      filters.add(new Filter(Filter.Op.EQ, FROM_ENTITY_TYPE, filter.getFromEntityType()));
    }
    if (StringUtils.isNotEmpty(filter.getToEntityType())) {
      filters.add(new Filter(Filter.Op.EQ, TO_ENTITY_TYPE, filter.getToEntityType()));
    }

    if (StringUtils.isNotEmpty(tenantId)) {
      filters.add(getTenantIdFilter(tenantId, useTenantHierarchy));
    }
    if (!filter.getNameList().isEmpty()) {
      filters.add(new Filter(Op.IN, NAME, filter.getNameList()));
    }
    if (!filter.getMultiplicityKind().equals(MultiplicityKind.UNRECOGNIZED)
        && !filter.getMultiplicityKind().equals(MultiplicityKind.UNKNOWN)) {
      filters.add(new Filter(Filter.Op.EQ, MULTIPLICITY_KIND, filter.getMultiplicityKind().name()));
    }

    // TODO: Not handling other fields in the filter for now since we aren't sure yet whether
    // we need the multiplicity or now.

    if (!filters.isEmpty()) {
      if (filters.size() == 1) {
        docStoreQuery.setFilter(filters.get(0));
      } else {
        Filter f = new Filter();
        f.setOp(Filter.Op.AND);
        f.setChildFilters(filters.toArray(new Filter[]{}));
        docStoreQuery.setFilter(f);
      }
    }
    return docStoreQuery;
  }
}
