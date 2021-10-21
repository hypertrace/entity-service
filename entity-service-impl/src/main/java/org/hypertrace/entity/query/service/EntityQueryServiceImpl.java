package org.hypertrace.entity.query.service;

import static java.util.Objects.isNull;
import static org.hypertrace.entity.query.service.EntityAttributeMapping.ENTITY_ATTRIBUTE_DOC_PREFIX;
import static org.hypertrace.entity.service.constants.EntityCollectionConstants.RAW_ENTITIES_COLLECTION;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ServiceException;
import com.typesafe.config.Config;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.DocumentParser;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Query;
import org.hypertrace.entity.query.service.v1.BulkEntityArrayAttributeUpdateRequest;
import org.hypertrace.entity.query.service.v1.BulkEntityUpdateRequest;
import org.hypertrace.entity.query.service.v1.BulkEntityUpdateRequest.EntityUpdateInfo;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.ColumnMetadata;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc.EntityQueryServiceImplBase;
import org.hypertrace.entity.query.service.v1.EntityUpdateRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.ResultSetMetadata;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.SetAttribute;
import org.hypertrace.entity.query.service.v1.TotalEntitiesRequest;
import org.hypertrace.entity.query.service.v1.TotalEntitiesResponse;
import org.hypertrace.entity.query.service.v1.UpdateOperation;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.service.constants.EntityServiceConstants;
import org.hypertrace.entity.service.util.DocStoreConverter;
import org.hypertrace.entity.service.util.DocStoreJsonFormat;
import org.hypertrace.entity.service.util.DocStoreJsonFormat.Parser;
import org.hypertrace.entity.service.util.DocStoreJsonFormat.Printer;
import org.hypertrace.entity.service.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityQueryServiceImpl extends EntityQueryServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(EntityQueryServiceImpl.class);
  private static final Parser PARSER = DocStoreJsonFormat.parser().ignoringUnknownFields();
  private static final Printer PRINTER = DocStoreJsonFormat.printer().includingDefaultValueFields();
  private static final DocumentParser DOCUMENT_PARSER = new DocumentParser();
  private static final String CHUNK_SIZE_CONFIG = "entity.query.service.response.chunk.size";
  private static final int DEFAULT_CHUNK_SIZE = 10_000;
  private static final String ARRAY_VALUE_PATH_SUFFIX = ".valueList.values";

  private final Collection entitiesCollection;
  private final EntityQueryConverter entityQueryConverter;
  private final EntityAttributeMapping entityAttributeMapping;
  private final int CHUNK_SIZE;

  public EntityQueryServiceImpl(
      Datastore datastore, Config config, GrpcChannelRegistry channelRegistry) {
    this(
        datastore.getCollection(RAW_ENTITIES_COLLECTION),
        new EntityAttributeMapping(config, channelRegistry),
        !config.hasPathOrNull(CHUNK_SIZE_CONFIG)
            ? DEFAULT_CHUNK_SIZE
            : config.getInt(CHUNK_SIZE_CONFIG));
  }

  public EntityQueryServiceImpl(
      Collection entitiesCollection, EntityAttributeMapping entityAttributeMapping, int chunkSize) {
    this.entitiesCollection = entitiesCollection;
    this.entityAttributeMapping = entityAttributeMapping;
    this.entityQueryConverter = new EntityQueryConverter(entityAttributeMapping);
    this.CHUNK_SIZE = chunkSize;
  }

  @Override
  public void execute(EntityQueryRequest request, StreamObserver<ResultSetChunk> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    Optional<String> tenantId = requestContext.getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    // TODO: Optimize this later. For now converting to EDS Query and then again to DocStore Query.
    Query query = entityQueryConverter.convertToEDSQuery(requestContext, request);
    /**
     * {@link EntityQueryRequest} selections need to treated differently, since they don't transform
     * one to one to {@link org.hypertrace.entity.data.service.v1.EntityDataRequest} selections
     */
    List<String> docStoreSelections =
        entityQueryConverter.convertSelectionsToDocStoreSelections(
            requestContext, request.getSelectionList());
    Iterator<Document> documentIterator =
        entitiesCollection.search(
            DocStoreConverter.transform(tenantId.get(), query, docStoreSelections));

    ResultSetMetadata resultSetMetadata =
        ResultSetMetadata.newBuilder()
            .addAllColumnMetadata(
                () ->
                    request.getSelectionList().stream()
                        .map(Expression::getColumnIdentifier)
                        .map(ColumnIdentifier::getColumnName)
                        .map(s -> ColumnMetadata.newBuilder().setColumnName(s).build())
                        .iterator())
            .build();
    if (!documentIterator.hasNext()) {
      ResultSetChunk.Builder resultBuilder = ResultSetChunk.newBuilder();
      resultBuilder.setResultSetMetadata(resultSetMetadata);
      resultBuilder.setIsLastChunk(true);
      resultBuilder.setChunkId(0);
      responseObserver.onNext(resultBuilder.build());
      responseObserver.onCompleted();
      return;
    }
    boolean isNewChunk = true;
    int chunkId = 0, rowCount = 0;
    ResultSetChunk.Builder resultBuilder = ResultSetChunk.newBuilder();
    while (documentIterator.hasNext()) {
      Optional<Entity> entity =
          DOCUMENT_PARSER.parseOrLog(documentIterator.next(), Entity.newBuilder());
      // Set metadata for new chunk
      if (isNewChunk) {
        resultBuilder.setResultSetMetadata(resultSetMetadata);
        isNewChunk = false;
      }
      if (entity.isPresent()) {
        // Build data
        resultBuilder.addRow(
            convertToEntityQueryResult(requestContext, entity.get(), request.getSelectionList()));
        rowCount++;
      }
      // current chunk is complete
      if (rowCount >= CHUNK_SIZE || !documentIterator.hasNext()) {
        resultBuilder.setChunkId(chunkId++);
        resultBuilder.setIsLastChunk(!documentIterator.hasNext());
        responseObserver.onNext(resultBuilder.build());
        resultBuilder = ResultSetChunk.newBuilder();
        isNewChunk = true;
        rowCount = 0;
      }
    }
    responseObserver.onCompleted();
  }

  private List<Entity> convertDocsToEntities(Iterator<Document> documentIterator) {
    List<Entity> entities = new ArrayList<>();
    while (documentIterator.hasNext()) {
      Document next = documentIterator.next();
      Entity.Builder builder = Entity.newBuilder();
      try {
        PARSER.merge(next.toJson(), builder);
      } catch (InvalidProtocolBufferException e) {
        LOG.error("Could not deserialize the document into an entity.", e);
      }
      entities.add(builder.build());
    }
    return entities;
  }

  private ResultSetChunk convertEntitiesToResultSetChunk(
      RequestContext requestContext, List<Entity> entities, List<Expression> selections) {

    ResultSetChunk.Builder resultBuilder = ResultSetChunk.newBuilder();
    // Build metadata
    resultBuilder.setResultSetMetadata(
        ResultSetMetadata.newBuilder()
            .addAllColumnMetadata(
                () ->
                    selections.stream()
                        .map(Expression::getColumnIdentifier)
                        .map(ColumnIdentifier::getColumnName)
                        .map(s -> ColumnMetadata.newBuilder().setColumnName(s).build())
                        .iterator())
            .build());
    // Build data
    resultBuilder.addAllRow(
        () ->
            entities.stream()
                .map(entity -> convertToEntityQueryResult(requestContext, entity, selections))
                .iterator());

    return resultBuilder.build();
  }

  Row convertToEntityQueryResult(
      RequestContext requestContext, Entity entity, List<Expression> selections) {
    Row.Builder result = Row.newBuilder();
    selections.stream()
        .filter(expression -> expression.getValueCase() == ValueCase.COLUMNIDENTIFIER)
        .forEach(
            expression -> {
              String columnName = expression.getColumnIdentifier().getColumnName();
              String edsSubDocPath =
                  entityAttributeMapping
                      .getDocStorePathByAttributeId(requestContext, columnName)
                      .orElse(null);
              if (edsSubDocPath != null) {
                // Map the attr name to corresponding Attribute Key in EDS and get the EDS
                // AttributeValue
                if (edsSubDocPath.equals(EntityServiceConstants.ENTITY_ID)) {
                  result.addColumn(
                      Value.newBuilder()
                          .setValueType(ValueType.STRING)
                          .setString(entity.getEntityId())
                          .build());
                } else if (edsSubDocPath.equals(EntityServiceConstants.ENTITY_NAME)) {
                  result.addColumn(
                      Value.newBuilder()
                          .setValueType(ValueType.STRING)
                          .setString(entity.getEntityName())
                          .build());
                } else if (edsSubDocPath.startsWith(ENTITY_ATTRIBUTE_DOC_PREFIX)) {
                  // Convert EDS AttributeValue to Gateway Value
                  AttributeValue attributeValue =
                      entity.getAttributesMap().get(edsSubDocPath.split("\\.")[1]);
                  result.addColumn(
                      EntityQueryConverter.convertAttributeValueToQueryValue(attributeValue));
                }
              } else {
                LOG.warn("columnName {} missing in attrNameToEDSAttrMap", columnName);
                result.addColumn(Value.getDefaultInstance());
              }
            });
    return result.build();
  }

  @Override
  public void update(EntityUpdateRequest request, StreamObserver<ResultSetChunk> responseObserver) {
    // Validations
    RequestContext requestContext = RequestContext.CURRENT.get();
    Optional<String> maybeTenantId = requestContext.getTenantId();
    if (maybeTenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }
    if (StringUtils.isEmpty(request.getEntityType())) {
      responseObserver.onError(new ServiceException("Entity type is missing in the request."));
      return;
    }
    if (request.getEntityIdsCount() == 0) {
      responseObserver.onError(new ServiceException("Entity IDs are missing in the request."));
    }
    if (!request.hasOperation()) {
      responseObserver.onError(new ServiceException("Operation is missing in the request."));
    }

    try {
      // Execute the update
      doUpdate(requestContext, request);

      // Finally return the selections
      List<Entity> entities =
          getProjectedEntities(
              request.getEntityIdsList(), request.getSelectionList(), requestContext);
      responseObserver.onNext(
          convertEntitiesToResultSetChunk(requestContext, entities, request.getSelectionList()));
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(
          new ServiceException("Error occurred while executing " + request, e));
    }
  }

  private void doUpdate(RequestContext requestContext, EntityUpdateRequest request)
      throws Exception {
    if (request.getOperation().hasSetAttribute()) {
      SetAttribute setAttribute = request.getOperation().getSetAttribute();
      String attributeId = setAttribute.getAttribute().getColumnName();

      String subDocPath =
          entityAttributeMapping
              .getDocStorePathByAttributeId(requestContext, attributeId)
              .orElseThrow(
                  () -> new IllegalArgumentException("Unknown attribute FQN " + attributeId));

      JSONDocument jsonDocument = convertToJsonDocument(setAttribute.getValue());

      Map<Key, Map<String, Document>> entitiesUpdateMap = new HashMap<>();
      for (String entityId : request.getEntityIdsList()) {
        SingleValueKey key =
            new SingleValueKey(requestContext.getTenantId().orElseThrow(), entityId);
        if (entitiesUpdateMap.containsKey(key)) {
          entitiesUpdateMap.get(key).put(subDocPath, jsonDocument);
        } else {
          Map<String, Document> subDocument = new HashMap<>();
          subDocument.put(subDocPath, jsonDocument);
          entitiesUpdateMap.put(key, subDocument);
        }
      }
      try {
        entitiesCollection.bulkUpdateSubDocs(entitiesUpdateMap);
      } catch (Exception e) {
        LOG.error(
            "Failed to update entities {}, subDocPath {}, with new doc {}.",
            entitiesUpdateMap,
            subDocPath,
            jsonDocument,
            e);
        throw e;
      }
    }
  }

  @Override
  public void bulkUpdate(
      BulkEntityUpdateRequest request, StreamObserver<ResultSetChunk> responseObserver) {
    // Validations
    RequestContext requestContext = RequestContext.CURRENT.get();
    Optional<String> maybeTenantId = requestContext.getTenantId();
    if (maybeTenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }
    if (StringUtils.isEmpty(request.getEntityType())) {
      responseObserver.onError(new ServiceException("Entity type is missing in the request."));
      return;
    }
    if (request.getEntitiesCount() == 0) {
      responseObserver.onError(new ServiceException("Entities are missing in the request."));
    }
    Map<String, EntityUpdateInfo> entitiesMap = request.getEntitiesMap();
    try {
      doBulkUpdate(requestContext, entitiesMap);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(
          new ServiceException("Error occurred while executing " + request, e));
    }
  }

  @Override
  public void bulkUpdateEntityArrayAttribute(
      BulkEntityArrayAttributeUpdateRequest request,
      StreamObserver<ResultSetChunk> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    String tenantId = requestContext.getTenantId().orElse(null);
    if (isNull(tenantId)) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }
    try {
      Set<Key> keys =
          request.getEntityIdsList().stream()
              .map(entityId -> new SingleValueKey(tenantId, entityId))
              .collect(Collectors.toSet());

      String attributeId = request.getAttribute().getColumnName();

      String subDocPath =
          entityAttributeMapping
              .getDocStorePathByAttributeId(requestContext, attributeId)
              .orElseThrow(
                  () -> new IllegalArgumentException("Unknown attribute FQN " + attributeId));

      List<Document> subDocuments = new ArrayList<>();
      for (LiteralConstant literalConstant : request.getValuesList()) {
        subDocuments.add(convertToJsonDocument(literalConstant));
      }
      BulkArrayValueUpdateRequest bulkArrayValueUpdateRequest =
          new BulkArrayValueUpdateRequest(
              keys,
              subDocPath + ARRAY_VALUE_PATH_SUFFIX,
              getMatchingOperation(request.getOperation()),
              subDocuments);
      entitiesCollection.bulkOperationOnArrayValue(bulkArrayValueUpdateRequest);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  private BulkArrayValueUpdateRequest.Operation getMatchingOperation(
      BulkEntityArrayAttributeUpdateRequest.Operation operation) {
    switch (operation) {
      case OPERATION_ADD:
        return BulkArrayValueUpdateRequest.Operation.ADD;
      case OPERATION_REMOVE:
        return BulkArrayValueUpdateRequest.Operation.REMOVE;
      case OPERATION_SET:
        return BulkArrayValueUpdateRequest.Operation.SET;
      default:
        throw new UnsupportedOperationException("Unknow operation " + operation);
    }
  }

  private JSONDocument convertToJsonDocument(LiteralConstant literalConstant) throws IOException {
    // Convert setAttribute LiteralConstant to AttributeValue. Need to be able to store an array
    // literal constant as an array
    AttributeValue attributeValue =
        EntityQueryConverter.convertToAttributeValue(literalConstant).build();
    String jsonValue = PRINTER.print(attributeValue);
    return new JSONDocument(jsonValue);
  }

  private List<Entity> getProjectedEntities(
      Iterable<String> entityIdsList,
      List<Expression> selectionList,
      RequestContext requestContext) {
    Query entitiesQuery = Query.newBuilder().addAllEntityId(entityIdsList).build();
    List<String> docStoreSelections =
        entityQueryConverter.convertSelectionsToDocStoreSelections(requestContext, selectionList);
    Iterator<Document> documentIterator =
        entitiesCollection.search(
            DocStoreConverter.transform(
                requestContext.getTenantId().orElseThrow(), entitiesQuery, docStoreSelections));
    return convertDocsToEntities(documentIterator);
  }

  private void doBulkUpdate(
      RequestContext requestContext, Map<String, EntityUpdateInfo> entitiesMap) throws Exception {
    Map<Key, Map<String, Document>> entitiesUpdateMap = new HashMap<>();
    for (String entityId : entitiesMap.keySet()) {
      Map<String, Document> transformedUpdateOperations =
          transformUpdateOperations(
              entitiesMap.get(entityId).getUpdateOperationList(), requestContext);
      if (transformedUpdateOperations.isEmpty()) {
        continue;
      }
      entitiesUpdateMap.put(
          new SingleValueKey(requestContext.getTenantId().orElseThrow(), entityId),
          transformedUpdateOperations);
    }

    if (entitiesUpdateMap.isEmpty()) {
      LOG.error("There are no entities to update!");
      return;
    }

    try {
      entitiesCollection.bulkUpdateSubDocs(entitiesUpdateMap);
    } catch (Exception e) {
      LOG.error("Failed to update entities {}", entitiesMap, e);
      throw e;
    }
  }

  private Map<String, Document> transformUpdateOperations(
      List<UpdateOperation> updateOperationList, RequestContext requestContext) throws Exception {
    Map<String, Document> documentMap = new HashMap<>();
    for (UpdateOperation updateOperation : updateOperationList) {
      if (!updateOperation.hasSetAttribute()) {
        continue;
      }
      SetAttribute setAttribute = updateOperation.getSetAttribute();
      String attributeId = setAttribute.getAttribute().getColumnName();
      String subDocPath =
          entityAttributeMapping
              .getDocStorePathByAttributeId(requestContext, attributeId)
              .orElseThrow(
                  () -> new IllegalArgumentException("Unknown attribute FQN " + attributeId));
      try {
        documentMap.put(subDocPath, convertToJsonDocument(setAttribute.getValue()));
      } catch (Exception e) {
        LOG.error("Failed to put update corresponding to {} in the documentMap", subDocPath, e);
        throw e;
      }
    }
    return Collections.unmodifiableMap(documentMap);
  }

  @Override
  public void total(
      TotalEntitiesRequest request, StreamObserver<TotalEntitiesResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    Optional<String> tenantId = requestContext.getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    // converting total entities request to entity query request
    EntityQueryRequest entityQueryRequest =
        EntityQueryRequest.newBuilder()
            .setEntityType(request.getEntityType())
            .setFilter(request.getFilter())
            .build();

    // converting entity query request to entity data service query
    Query query = entityQueryConverter.convertToEDSQuery(requestContext, entityQueryRequest);
    long total =
        entitiesCollection.total(
            DocStoreConverter.transform(tenantId.get(), query, Collections.emptyList()));
    responseObserver.onNext(TotalEntitiesResponse.newBuilder().setTotal(total).build());
    responseObserver.onCompleted();
  }
}
