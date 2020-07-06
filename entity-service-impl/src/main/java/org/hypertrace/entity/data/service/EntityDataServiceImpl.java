package org.hypertrace.entity.data.service;

import static org.hypertrace.entity.service.constants.EntityCollectionConstants.ENRICHED_ENTITIES_COLLECTION;
import static org.hypertrace.entity.service.constants.EntityCollectionConstants.ENTITY_RELATIONSHIPS_COLLECTION;
import static org.hypertrace.entity.service.constants.EntityCollectionConstants.ENTITY_TYPES_COLLECTION;
import static org.hypertrace.entity.service.constants.EntityCollectionConstants.RAW_ENTITIES_COLLECTION;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.ByIdRequest;
import org.hypertrace.entity.data.service.v1.ByTypeAndIdentifyingAttributes;
import org.hypertrace.entity.data.service.v1.Empty;
import org.hypertrace.entity.data.service.v1.EnrichedEntities;
import org.hypertrace.entity.data.service.v1.EnrichedEntity;
import org.hypertrace.entity.data.service.v1.Entities;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc.EntityDataServiceImplBase;
import org.hypertrace.entity.data.service.v1.EntityRelationship;
import org.hypertrace.entity.data.service.v1.EntityRelationships;
import org.hypertrace.entity.data.service.v1.Query;
import org.hypertrace.entity.data.service.v1.RelationshipsQuery;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.constants.EntityServiceConstants;
import org.hypertrace.entity.service.exception.InvalidRequestException;
import org.hypertrace.entity.service.util.DocStoreConverter;
import org.hypertrace.entity.service.util.DocStoreJsonFormat;
import org.hypertrace.entity.service.util.DocStoreJsonFormat.Parser;
import org.hypertrace.entity.service.util.StringUtils;
import org.hypertrace.entity.service.util.TenantUtils;
import org.hypertrace.entity.service.util.UUIDGenerator;
import org.hypertrace.entity.type.service.v1.AttributeType;
import org.hypertrace.entity.type.service.v1.EntityType;
import org.hypertrace.entity.type.service.v1.EntityType.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EntityDataService implementation with CRUD and query operations for entities on the doc store.
 */
public class EntityDataServiceImpl extends EntityDataServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(EntityDataServiceImpl.class);
  private static final Parser PARSER = DocStoreJsonFormat.parser().ignoringUnknownFields();
  private static final DocStoreJsonFormat.Printer PRINTER = DocStoreJsonFormat.printer();

  private final Collection entityTypesCollection;
  private final Collection entitiesCollection;
  private final Collection relationshipsCollection;
  private final Collection enrichedEntitiesCollection;

  public EntityDataServiceImpl(Datastore datastore) {
    this.entityTypesCollection = datastore
        .getCollection(ENTITY_TYPES_COLLECTION);
    this.entitiesCollection = datastore.getCollection(RAW_ENTITIES_COLLECTION);
    this.relationshipsCollection = datastore
        .getCollection(ENTITY_RELATIONSHIPS_COLLECTION);
    this.enrichedEntitiesCollection = datastore
        .getCollection(ENRICHED_ENTITIES_COLLECTION);
  }

  /**
   * <p>Creates or Updates an Entity</p>
   * <br/> If the entityId is provided it is used as is to update the Entity. If the identifying
   * attributes are provided, then the entityId is generated from them If none of the above are
   * provided, we error out. The ID of the entity is generated from its identifying attributes.
   *
   * @param request          Entity to be created
   * @param responseObserver Observer to be notified on about the Entity creation request
   */
  @Override
  public void upsert(Entity request,
      io.grpc.stub.StreamObserver<Entity> responseObserver) {
    String tenantId = RequestContext.CURRENT.get().getTenantId().orElse(null);
    if (tenantId == null) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    // The entityId should be empty while creating the entity since the service is supposed to
    // assign a unique identifier to the entity.
    if (StringUtils.isEmpty(request.getEntityType())) {
      LOG.info("{}. Invalid upsert request:{}", request, ErrorMessages.ENTITY_TYPE_EMPTY);
      responseObserver.onError(new RuntimeException(ErrorMessages.ENTITY_TYPE_EMPTY));
      return;
    }

    // Validate if all identifying attributes are present in the incoming request
    Map<String, AttributeValue> idAttrMap = validateAndGetIdentifyingAttributes(tenantId, request);
    if (idAttrMap.isEmpty()) {
      responseObserver.onError(
          new RuntimeException(
              "All required identifying attributes not present for Entity:" + request));
      return;
    }

    // UUID is generated from identifying attributes.
    String entityId = generateEntityId(tenantId,
        request.getEntityType(), request.getIdentifyingAttributesMap());

    //Copy over the identify attributes to other attributes
    Entity entityWithEntityId = Entity.newBuilder(request)
        .putAllIdentifyingAttributes(idAttrMap)
        .putAllAttributes(request.getIdentifyingAttributesMap())
        .setEntityId(entityId)
        .setTenantId(tenantId)
        .build();

    upsertEntity(tenantId, entityId, entityWithEntityId,
        Entity.newBuilder(), entitiesCollection, responseObserver);
  }

  @Override
  public void upsertEntities(Entities request, StreamObserver<Empty> responseObserver) {
    String tenantId = RequestContext.CURRENT.get().getTenantId().orElse(null);
    if (tenantId == null) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    Map<String, Entity> entities = new HashMap<>();
    for (Entity entity : request.getEntityList()) {
      // The entityId should be empty while creating the entity since the service is supposed to
      // assign a unique identifier to the entity.
      if (StringUtils.isEmpty(entity.getEntityType())) {
        LOG.info("{}. Invalid upsertEntities request:{}", request, ErrorMessages.ENTITY_TYPE_EMPTY);
        responseObserver.onError(new RuntimeException(ErrorMessages.ENTITY_TYPE_EMPTY));
        return;
      }

      // Validate if all identifying attributes are present in the incoming request
      Map<String, AttributeValue> idAttrMap = validateAndGetIdentifyingAttributes(tenantId,
          entity);
      if (idAttrMap.isEmpty()) {
        responseObserver.onError(
            new RuntimeException(
                "All required identifying attributes not present for Entity:" + entity));
        return;
      }

      // UUID is generated from identifying attributes.
      String entityId = generateEntityId(tenantId,
          entity.getEntityType(), entity.getIdentifyingAttributesMap());

      // Copy over the identify attributes to other attributes
      Entity entityWithEntityId =
          Entity.newBuilder(entity)
              .putAllIdentifyingAttributes(idAttrMap)
              .putAllAttributes(entity.getIdentifyingAttributesMap())
              .setEntityId(entityId)
              .setTenantId(tenantId)
              .build();

      entities.put(entityId, entityWithEntityId);
    }

    upsertEntities(tenantId, entities, entitiesCollection, responseObserver);
  }

  /**
   * <p>Get an Entity by the EntityId and EntityType</p>
   *
   * @param request          ID of the entity which constitutes the EntityType and EntityID(UUID)
   * @param responseObserver Observer to be notified on about the Entity get request
   */
  @Override
  public void getById(ByIdRequest request, StreamObserver<Entity> responseObserver) {
    try {
      validate(request);
    } catch (InvalidRequestException e) {
      responseObserver.onError(e);
      return;
    }

    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    searchByIdAndStreamSingleResponse(tenantId.get(),
        request.getEntityId(), entitiesCollection, Entity.newBuilder(), responseObserver);
  }

  /**
   * <p>Get an Entity by the EntityType and its identifying attributes</p>
   *
   * @param request          ID of the entity which constitutes the EntityType and EntityID(UUID)
   * @param responseObserver Observer to be notified on about the Entity get request
   */
  @Override
  public void getByTypeAndIdentifyingProperties(
      ByTypeAndIdentifyingAttributes request, StreamObserver<Entity> responseObserver) {
    try {
      validate(request);
    } catch (InvalidRequestException e) {
      responseObserver.onError(e);
      return;
    }

    String tenantId = RequestContext.CURRENT.get().getTenantId().orElse(null);
    if (tenantId == null) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    String entityId = generateEntityId(tenantId, request.getEntityType(),
        request.getIdentifyingAttributesMap());
    searchByIdAndStreamSingleResponse(tenantId, entityId, entitiesCollection,
        Entity.newBuilder(), responseObserver);
  }

  /**
   * <p>Deletes an Entity by the EntityId and EntityType</p>
   *
   * @param request          ID of the entity to be deleted
   * @param responseObserver Observer to be notified on about the Entity delete request
   */
  @Override
  public void delete(ByIdRequest request,
      io.grpc.stub.StreamObserver<Empty> responseObserver) {
    if (StringUtils.isEmpty(request.getEntityId())) {
      LOG.info("{}. Invalid delete request:{}", request, ErrorMessages.ENTITY_ID_EMPTY);
      responseObserver.onError(new RuntimeException(ErrorMessages.ENTITY_ID_EMPTY));
      return;
    }

    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    SingleValueKey key = new SingleValueKey(tenantId.get(), request.getEntityId());

    if (entitiesCollection.delete(key)) {
      responseObserver.onNext(Empty.newBuilder().build());
      responseObserver.onCompleted();
    } else {
      responseObserver.onError(new RuntimeException("Could not delete the entity."));
    }
  }

  /**
   * <p>Fetch entities by applying filters</p>
   *
   * @param request          Query filters to be applied for filtering entities
   * @param responseObserver Observer to be notified on about the Entity query request
   */
  @Override
  public void query(Query request, StreamObserver<Entity> responseObserver) {
    logQuery(request);
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    searchByQueryAndStreamResponse(DocStoreConverter.transform(tenantId.get(), request),
        responseObserver,
        tenantId.get());
  }

  @Override
  public void upsertRelationships(
      EntityRelationships request, StreamObserver<Empty> responseObserver) {
    String tenantId = RequestContext.CURRENT.get().getTenantId().orElse(null);
    if (tenantId == null) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    Map<Key, Document> entityRelations = new HashMap<>();
    try {

      for (EntityRelationship relationship : request.getRelationshipList()) {
        if (StringUtils.isEmpty(relationship.getFromEntityId())
            || StringUtils.isEmpty(relationship.getToEntityId())
            || StringUtils.isEmpty(relationship.getEntityRelationshipType())) {
          LOG.warn("Invalid relationship upsert request:{}", relationship);
          continue;
        }

        Document document = convertEntityRelationshipToDocument(tenantId, relationship);
        Key key =
            new EntityRelationshipDocKey(tenantId,
                relationship.getEntityRelationshipType(),
                relationship.getFromEntityId(),
                relationship.getToEntityId());

        entityRelations.put(key, document);
      }

      boolean status = relationshipsCollection.bulkUpsert(entityRelations);
      if (status) {
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
      } else {
        responseObserver.onError(new RuntimeException("Could not bulk upsert relationships."));
      }
    } catch (IOException e) {
      LOG.error("Failed to bulk upsert relationships.", e);
      responseObserver.onError(e);
    }
  }

  @Override
  public void getRelationships(RelationshipsQuery query,
      StreamObserver<EntityRelationship> responseObserver) {
    logQuery(query);

    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    org.hypertrace.core.documentstore.Query docStoreQuery = new org.hypertrace.core.documentstore.Query();
    List<Filter> filters = new ArrayList<>();
    filters.add(DocStoreConverter.getTenantIdEqFilter(tenantId.get()));
    if (query.getEntityRelationshipCount() > 0) {
      filters.add(new Filter(Filter.Op.IN, EntityServiceConstants.ENTITY_RELATIONSHIP_TYPE,
          query.getEntityRelationshipList()));
    }
    if (query.getFromEntityIdCount() > 0) {
      filters.add(new Filter(Filter.Op.IN, EntityServiceConstants.FROM_ENTITY_ID,
          query.getFromEntityIdList()));
    }
    if (query.getToEntityIdCount() > 0) {
      filters.add(
          new Filter(Filter.Op.IN, EntityServiceConstants.TO_ENTITY_ID, query.getToEntityIdList()));
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

    searchByQueryAndStreamRelationships(docStoreQuery, responseObserver, tenantId.get());
  }

  @Override
  public void upsertEnrichedEntity(EnrichedEntity request,
      StreamObserver<EnrichedEntity> responseObserver) {
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    if (StringUtils.isEmpty(request.getEntityType())) {
      LOG.info("{}. Invalid upsertEnrichedEntity request:{}", request,
          ErrorMessages.ENTITY_TYPE_EMPTY);
      responseObserver.onError(new RuntimeException(ErrorMessages.ENTITY_TYPE_EMPTY));
      return;
    }

    if (StringUtils.isEmpty(request.getEntityId())) {
      LOG.info("{}. Invalid upsertEnrichedEntity request:{}", request,
          ErrorMessages.ENTITY_ID_EMPTY);
      responseObserver.onError(new RuntimeException(ErrorMessages.ENTITY_ID_EMPTY));
      return;
    }

    upsertEntity(tenantId.get(), request.getEntityId(), request,
        EnrichedEntity.newBuilder(), enrichedEntitiesCollection, responseObserver);
  }

  @Override
  public void upsertEnrichedEntities(EnrichedEntities request,
      StreamObserver<Empty> responseObserver) {
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    for (EnrichedEntity entity : request.getEntitiesList()) {
      if (StringUtils.isEmpty(entity.getEntityType())) {
        LOG.info("{}. Invalid upsertEnrichedEntities request:{}", entity,
            ErrorMessages.ENTITY_TYPE_EMPTY);
        responseObserver.onError(new RuntimeException(ErrorMessages.ENTITY_TYPE_EMPTY));
        return;
      }

      if (StringUtils.isEmpty(entity.getEntityId())) {
        LOG.info("{}. Invalid upsertEnrichedEntities request:{}", entity,
            ErrorMessages.ENTITY_ID_EMPTY);
        responseObserver.onError(new RuntimeException(ErrorMessages.ENTITY_ID_EMPTY));
        return;
      }
    }

    Map<String, EnrichedEntity> entityMap = request.getEntitiesList().stream()
        .collect(Collectors.toMap(EnrichedEntity::getEntityId, e -> e));

    upsertEntities(tenantId.get(), entityMap, enrichedEntitiesCollection, responseObserver);
  }

  @Override
  public void getEnrichedEntityById(ByIdRequest request,
      StreamObserver<EnrichedEntity> responseObserver) {
    try {
      validate(request);
    } catch (InvalidRequestException e) {
      responseObserver.onError(e);
      return;
    }

    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    searchByIdAndStreamSingleResponse(tenantId.get(),
        request.getEntityId(), enrichedEntitiesCollection, EnrichedEntity.newBuilder(),
        responseObserver);
  }

  @Override
  public void getEnrichedEntityByTypeAndIdentifyingProps(ByTypeAndIdentifyingAttributes request,
      StreamObserver<EnrichedEntity> responseObserver) {
    try {
      validate(request);
    } catch (InvalidRequestException e) {
      responseObserver.onError(e);
      return;
    }

    String tenantId = RequestContext.CURRENT.get().getTenantId().orElse(null);
    if (tenantId == null) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    String entityId = generateEntityId(tenantId,
        request.getEntityType(), request.getIdentifyingAttributesMap());
    searchByIdAndStreamSingleResponse(tenantId, entityId,
        enrichedEntitiesCollection, EnrichedEntity.newBuilder(), responseObserver);
  }

  private String generateEntityId(String tenantId, String entityType,
      Map<String, AttributeValue> attributeMap) {
    Map<String, AttributeValue> map = new HashMap<>(attributeMap);
    // Add the tenantId and entityType to the map to make it more unique.
    map.put("customerId",
        AttributeValue.newBuilder().setValue(Value.newBuilder().setString(tenantId)).build());
    map.put("entityType",
        AttributeValue.newBuilder().setValue(Value.newBuilder().setString(entityType)).build());
    return UUIDGenerator.generateUUID(map);
  }

  private void validate(ByIdRequest request) throws InvalidRequestException {
    if (StringUtils.isEmpty(request.getEntityId())) {
      LOG.info("{}. Invalid get request:{}", request, ErrorMessages.ENTITY_ID_EMPTY);
      throw new InvalidRequestException(ErrorMessages.ENTITY_ID_EMPTY);
    }
  }

  private void validate(ByTypeAndIdentifyingAttributes request) throws InvalidRequestException {
    if (StringUtils.isEmpty(request.getEntityType())) {
      LOG.info("{}. Invalid ByTypeAndIdentifyingAttributes request: {}",
          request, ErrorMessages.ENTITY_TYPE_EMPTY);
      throw new InvalidRequestException(ErrorMessages.ENTITY_TYPE_EMPTY);
    }

    if (request.getIdentifyingAttributesMap().isEmpty()) {
      LOG.info("{}. Invalid ByTypeAndIdentifyingAttributes request: {}",
          request, ErrorMessages.ENTITY_IDENTIFYING_ATTRS_EMPTY);
      throw new InvalidRequestException(ErrorMessages.ENTITY_IDENTIFYING_ATTRS_EMPTY);
    }
  }

  private <T extends GeneratedMessageV3> void upsertEntity(String tenantId, String entityId,
      T entity, Message.Builder builder, Collection collection,
      StreamObserver<T> responseObserver) {
    try {
      Document document = convertEntityToDocument(entity);
      collection.upsert(new SingleValueKey(tenantId, entityId), document);
      searchByIdAndStreamSingleResponse(tenantId, entityId,
          collection, builder, responseObserver);
    } catch (IOException e) {
      responseObserver.onError(new RuntimeException("Could not create entity.", e));
    }
  }

  private <T extends GeneratedMessageV3> void upsertEntities(String tenantId, Map<String, T> map,
      Collection collection, StreamObserver<Empty> responseObserver) {
    try {
      Map<Key, Document> entities = new HashMap<>();
      for (Map.Entry<String, T> entry : map.entrySet()) {
        Document doc = convertEntityToDocument(entry.getValue());
        SingleValueKey key = new SingleValueKey(tenantId, entry.getKey());
        entities.put(key, doc);
      }

      boolean status = collection.bulkUpsert(entities);
      if (status) {
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
      } else {
        responseObserver.onError(new RuntimeException("Failed to bulk upsert entities"));
      }
    } catch (IOException e) {
      LOG.error("Failed to bulk upsert entities", e);
      responseObserver.onError(e);
    }
  }

  private Map<String, AttributeValue> validateAndGetIdentifyingAttributes(String tenantId,
      Entity request) {
    Map<String, AttributeValue> idAttrMap = new HashMap<>();
    List<String> idAttrNames = getIdentifyingAttributesForEntityType(
        tenantId, request.getEntityType())
        .stream()
        .map(AttributeType::getName)
        .collect(Collectors.toList());
    if (idAttrNames.isEmpty()) {
      LOG.error("No identifying attributes defined for EntityType: {}", request.getEntityType());
      return Collections.emptyMap();
    }
    for (String attrName : idAttrNames) {
      AttributeValue attributeValue = request.getIdentifyingAttributesMap().get(attrName);
      if (attributeValue != null) {
        idAttrMap.put(attrName, attributeValue);
      } else {
        LOG.error("Required identifying attribute not present for Entity: {}, missingAttr: {}",
            request, attrName);
        return Collections.emptyMap();
      }
    }
    return idAttrMap;
  }

  private <T extends GeneratedMessageV3> JSONDocument convertEntityToDocument(T entity)
      throws IOException {
    try {
      return DocStoreConverter.transform(entity);
    } catch (IOException e) {
      LOG.error("Could not covert the attributes into JSON doc.", e);
      throw e;
    }
  }

  private JSONDocument convertEntityRelationshipToDocument(String tenantId,
      EntityRelationship relationship)
      throws IOException {
    try {
      String json = PRINTER.print(EntityRelationship.newBuilder(relationship)
          .setTenantId(tenantId)
          .build());
      return new JSONDocument(json);
    } catch (IOException e) {
      LOG.error("Could not covert the EntityRelationship into JSON doc.", e);
      throw e;
    }
  }

  private <T extends Message> void searchByIdAndStreamSingleResponse(
      String tenantId, String entityId, Collection collection, Message.Builder builder,
      StreamObserver<T> responseObserver) {
    org.hypertrace.core.documentstore.Query query = new org.hypertrace.core.documentstore.Query();
    String docId = new SingleValueKey(tenantId, entityId).toString();
    query.setFilter(new Filter(Filter.Op.EQ, EntityServiceConstants.ID, docId));

    Iterator<Document> result = collection.search(query);
    List<T> entities = new ArrayList<>();
    while (result.hasNext()) {
      Document next = result.next();
      Message.Builder b = builder.clone();
      try {
        PARSER.merge(next.toJson(), b);

        // Populate the tenant id field with the tenant id that's received for backward
        // compatibility.
        Descriptors.FieldDescriptor fieldDescriptor =
            b.getDescriptorForType().findFieldByName("tenant_id");
        if (fieldDescriptor != null) {
          b.setField(fieldDescriptor, tenantId);
        }
      } catch (InvalidProtocolBufferException e) {
        LOG.error("Could not deserialize the document into an entity.", e);
      }

      entities.add((T) b.build());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("MongoDB query has returned the result: {}", entities);
    }

    if (entities.size() == 1) {
      responseObserver.onNext(entities.get(0));
      responseObserver.onCompleted();
    } else if (entities.size() > 1) {
      responseObserver.onError(
          new IllegalStateException("Multiple entities with same id are found."));
    } else {
      // When there is no result, we should return the default instance, which is a way
      // of saying it's null.
      //TODO : Not convinced with the default instance
      responseObserver.onNext((T) builder.build());
      responseObserver.onCompleted();
    }
  }

  private void searchByQueryAndStreamResponse(org.hypertrace.core.documentstore.Query query,
      StreamObserver<Entity> responseObserver, String tenantId) {
    Iterator<Document> result = entitiesCollection.search(query);
    List<Entity> entities = new ArrayList<>();
    while (result.hasNext()) {
      Document next = result.next();
      Entity.Builder builder = Entity.newBuilder();
      try {
        PARSER.merge(next.toJson(), builder);

        // Populate the tenant id field with the tenant id that's received for backward
        // compatibility.
        builder.setTenantId(tenantId);
      } catch (InvalidProtocolBufferException e) {
        LOG.error("Could not deserialize the document into an entity.", e);
      }

      entities.add(builder.build());
      responseObserver.onNext(builder.build());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("MongoDB query has returned the result: {}", entities);
    }
    responseObserver.onCompleted();
  }

  private void searchByQueryAndStreamRelationships(org.hypertrace.core.documentstore.Query query,
      StreamObserver<EntityRelationship> responseObserver, String tenantId) {
    Iterator<Document> result = relationshipsCollection.search(query);
    List<EntityRelationship> relationships = new ArrayList<>();
    while (result.hasNext()) {
      Document next = result.next();
      EntityRelationship.Builder builder = EntityRelationship.newBuilder();
      try {
        PARSER.merge(next.toJson(), builder);

        // Populate the tenant id field with the tenant id that's received for backward
        // compatibility.
        builder.setTenantId(tenantId);
      } catch (InvalidProtocolBufferException e) {
        LOG.error("Could not deserialize the document into an EntityRelationship.", e);
      }

      relationships.add(builder.build());
      responseObserver.onNext(builder.build());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("MongoDB query has returned the result: {}", relationships);
    }
    responseObserver.onCompleted();
  }

  private void logQuery(Object query) {
    if (LOG.isDebugEnabled()) {
      LOG.info("Received query: {}", query.toString());
    }
  }

  private List<AttributeType> getIdentifyingAttributesForEntityType(
      String tenantId, String entityTypeName) {
    org.hypertrace.core.documentstore.Query query = new org.hypertrace.core.documentstore.Query();
    Filter entityTypeNameFilter = new Filter(Filter.Op.IN, EntityServiceConstants.ID,
        TenantUtils.getTenantHierarchy(tenantId).stream()
            .map(cid -> new SingleValueKey(cid, entityTypeName).toString())
            .collect(Collectors.toList()));
    query.setFilter(entityTypeNameFilter);

    Iterator<Document> entityTypes = entityTypesCollection.search(query);

    if (entityTypes.hasNext()) {
      //Since we are filtering on the name, it is safe to pick only the first
      Document entityTypeDoc = entityTypes.next();
      try {
        Builder builder = EntityType.newBuilder();
        PARSER.merge(entityTypeDoc.toJson(), builder);
        EntityType entityType = builder.build();
        return entityType.getAttributeTypeList().stream()
            .filter(AttributeType::getIdentifyingAttribute)
            .collect(Collectors.toList());
      } catch (IOException e) {
        LOG.error("Error processing entityType: {}", entityTypeDoc.toJson(), e);
      }
    }
    return Collections.emptyList();
  }

  private static class ErrorMessages {

    static final String ENTITY_ID_EMPTY = "Entity ID is empty";
    static final String ENTITY_TYPE_EMPTY = "Entity Type is empty";
    static final String ENTITY_IDENTIFYING_ATTRS_EMPTY = "Entity identifying attributes are empty";
  }
}
