package org.hypertrace.entity.query.service;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.expression.impl.LogicalExpression.or;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.NONE;
import static org.hypertrace.entity.attribute.translator.EntityAttributeMapping.ENTITY_ATTRIBUTE_DOC_PREFIX;
import static org.hypertrace.entity.data.service.v1.AttributeValue.VALUE_LIST_FIELD_NUMBER;
import static org.hypertrace.entity.data.service.v1.AttributeValueList.VALUES_FIELD_NUMBER;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING;
import static org.hypertrace.entity.service.constants.EntityCollectionConstants.RAW_ENTITIES_COLLECTION;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.google.protobuf.ServiceException;
import com.typesafe.config.Config;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.query.Filter;
import org.hypertrace.core.documentstore.query.Selection;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeChangeEvaluator;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.data.service.DocumentParser;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.AttributeValueList;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Query;
import org.hypertrace.entity.fetcher.EntityFetcher;
import org.hypertrace.entity.metric.EntityCounterMetricSender;
import org.hypertrace.entity.query.service.converter.AliasProvider;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.converter.ConverterModule;
import org.hypertrace.entity.query.service.converter.response.DocumentConverter;
import org.hypertrace.entity.query.service.v1.AttributeUpdateOperation;
import org.hypertrace.entity.query.service.v1.BulkEntityArrayAttributeUpdateRequest;
import org.hypertrace.entity.query.service.v1.BulkEntityArrayAttributeUpdateResponse;
import org.hypertrace.entity.query.service.v1.BulkEntityUpdateRequest;
import org.hypertrace.entity.query.service.v1.BulkEntityUpdateRequest.EntityUpdateInfo;
import org.hypertrace.entity.query.service.v1.BulkUpdateAllMatchingFilterRequest;
import org.hypertrace.entity.query.service.v1.BulkUpdateAllMatchingFilterResponse;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.ColumnMetadata;
import org.hypertrace.entity.query.service.v1.DeleteEntitiesRequest;
import org.hypertrace.entity.query.service.v1.DeleteEntitiesResponse;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc.EntityQueryServiceImplBase;
import org.hypertrace.entity.query.service.v1.EntityUpdateRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.Function;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.ResultSetMetadata;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.SetAttribute;
import org.hypertrace.entity.query.service.v1.TotalEntitiesRequest;
import org.hypertrace.entity.query.service.v1.TotalEntitiesResponse;
import org.hypertrace.entity.query.service.v1.Update;
import org.hypertrace.entity.query.service.v1.UpdateOperation;
import org.hypertrace.entity.query.service.v1.UpdateSummary;
import org.hypertrace.entity.query.service.v1.UpdatedEntity;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.service.change.event.api.EntityChangeEventGenerator;
import org.hypertrace.entity.service.constants.EntityServiceConstants;
import org.hypertrace.entity.service.util.DocStoreConverter;
import org.hypertrace.entity.service.util.DocStoreJsonFormat;
import org.hypertrace.entity.service.util.DocStoreJsonFormat.Printer;
import org.hypertrace.entity.service.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityQueryServiceImpl extends EntityQueryServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(EntityQueryServiceImpl.class);
  private static final Printer PRINTER = DocStoreJsonFormat.printer().includingDefaultValueFields();
  private static final DocumentParser DOCUMENT_PARSER = new DocumentParser();
  private static final String CHUNK_SIZE_CONFIG = "entity.query.service.response.chunk.size";
  private static final String QUERY_AGGREGATION_ENABLED_CONFIG =
      "entity.service.config.query.aggregation.enabled";
  private static final String ENTITY_IDS_DELETE_LIMIT_CONFIG = "entity.delete.limit";

  private static final int DEFAULT_CHUNK_SIZE = 10_000;
  private static final String ARRAY_VALUE_PATH_SUFFIX =
      Stream.of(
              "",
              AttributeValue.getDescriptor()
                  .findFieldByNumber(VALUE_LIST_FIELD_NUMBER)
                  .getJsonName(),
              AttributeValueList.getDescriptor()
                  .findFieldByNumber(VALUES_FIELD_NUMBER)
                  .getJsonName())
          .collect(joining("."));
  private static final int MAX_STRING_LENGTH_FOR_UPDATE = 1000;

  private final Collection entitiesCollection;
  private final EntityQueryConverter entityQueryConverter;
  private final EntityAttributeMapping entityAttributeMapping;
  private final int CHUNK_SIZE;
  private final Injector injector;
  private final boolean queryAggregationEnabled;
  private final int maxEntitiesToDelete;
  private final EntityFetcher entityFetcher;
  private final EntityChangeEventGenerator entityChangeEventGenerator;
  private final EntityAttributeChangeEvaluator entityAttributeChangeEvaluator;
  private final EntityCounterMetricSender entityCounterMetricSender;

  public EntityQueryServiceImpl(
      Datastore datastore,
      Config config,
      EntityAttributeMapping entityAttributeMapping,
      EntityChangeEventGenerator entityChangeEventGenerator,
      EntityCounterMetricSender entityCounterMetricSender) {
    this(
        datastore.getCollection(RAW_ENTITIES_COLLECTION),
        entityAttributeMapping,
        entityChangeEventGenerator,
        new EntityAttributeChangeEvaluator(config, entityAttributeMapping),
        entityCounterMetricSender,
        !config.hasPathOrNull(CHUNK_SIZE_CONFIG)
            ? DEFAULT_CHUNK_SIZE
            : config.getInt(CHUNK_SIZE_CONFIG),
        config.hasPath(QUERY_AGGREGATION_ENABLED_CONFIG)
            && config.getBoolean(QUERY_AGGREGATION_ENABLED_CONFIG),
        config.hasPath(ENTITY_IDS_DELETE_LIMIT_CONFIG)
            ? config.getInt(ENTITY_IDS_DELETE_LIMIT_CONFIG)
            : 10000);
  }

  public EntityQueryServiceImpl(
      Collection entitiesCollection,
      EntityAttributeMapping entityAttributeMapping,
      EntityChangeEventGenerator entityChangeEventGenerator,
      EntityAttributeChangeEvaluator entityAttributeChangeEvaluator,
      EntityCounterMetricSender entityCounterMetricSender,
      int chunkSize,
      boolean queryAggregationEnabled,
      int maxEntitiesToDelete) {
    this(
        entitiesCollection,
        entityAttributeMapping,
        entityChangeEventGenerator,
        entityAttributeChangeEvaluator,
        entityCounterMetricSender,
        new EntityFetcher(entitiesCollection, DOCUMENT_PARSER),
        chunkSize,
        queryAggregationEnabled,
        maxEntitiesToDelete);
  }

  @VisibleForTesting
  EntityQueryServiceImpl(
      Collection entitiesCollection,
      EntityAttributeMapping entityAttributeMapping,
      EntityChangeEventGenerator entityChangeEventGenerator,
      EntityAttributeChangeEvaluator entityAttributeChangeEvaluator,
      EntityCounterMetricSender entityCounterMetricSender,
      EntityFetcher entityFetcher,
      int chunkSize,
      boolean queryAggregationEnabled,
      int maxEntitiesToDelete) {
    this.entitiesCollection = entitiesCollection;
    this.entityAttributeMapping = entityAttributeMapping;
    this.entityQueryConverter = new EntityQueryConverter(entityAttributeMapping);
    this.CHUNK_SIZE = chunkSize;
    this.injector = Guice.createInjector(new ConverterModule(entityAttributeMapping));
    this.queryAggregationEnabled = queryAggregationEnabled;
    this.maxEntitiesToDelete = maxEntitiesToDelete;
    this.entityChangeEventGenerator = entityChangeEventGenerator;
    this.entityFetcher = entityFetcher;
    this.entityAttributeChangeEvaluator = entityAttributeChangeEvaluator;
    this.entityCounterMetricSender = entityCounterMetricSender;
  }

  @Override
  public void execute(EntityQueryRequest request, StreamObserver<ResultSetChunk> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    Optional<String> tenantId = requestContext.getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    try (CloseableIterator<Document> documentIterator = searchDocuments(requestContext, request)) {
      streamResponse(request, responseObserver, requestContext, documentIterator);
    } catch (Exception ex) {
      LOG.error("Error while executing entity query request ", ex);
      responseObserver.onError(new ServiceException(ex));
    }
  }

  private void streamResponse(
      final EntityQueryRequest request,
      final StreamObserver<ResultSetChunk> responseObserver,
      final RequestContext requestContext,
      final CloseableIterator<Document> documentIterator)
      throws ConversionException {
    final DocumentConverter rowConverter = injector.getInstance(DocumentConverter.class);
    ResultSetMetadata resultSetMetadata;
    resultSetMetadata = this.buildMetadataForSelections(request.getSelectionList());

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
      // Set metadata for new chunk
      if (isNewChunk) {
        resultBuilder.setResultSetMetadata(resultSetMetadata);
        isNewChunk = false;
      }

      try {
        Optional<Row> row =
            convertToRow(
                requestContext,
                documentIterator.next(),
                resultSetMetadata,
                rowConverter,
                request.getSelectionList());
        if (row.isPresent()) {
          resultBuilder.addRow(row.get());
          rowCount++;
        }
      } catch (final Exception e) {
        responseObserver.onError(new ServiceException(e));
        return;
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

  private Optional<Row> convertToRow(
      RequestContext requestContext,
      Document document,
      ResultSetMetadata resultSetMetadata,
      DocumentConverter rowConverter,
      List<Expression> selectionList)
      throws ConversionException {
    final Row row;
    if (queryAggregationEnabled) {
      row = rowConverter.convertToRow(document, resultSetMetadata);
      return Optional.of(row);
    } else {
      Optional<Entity> entity = DOCUMENT_PARSER.parseOrLog(document, Entity.newBuilder());

      if (entity.isPresent()) {
        row = convertToEntityQueryResult(requestContext, entity.get(), selectionList);
        return Optional.of(row);
      }
    }
    return Optional.empty();
  }

  private CloseableIterator<Document> searchDocuments(
      RequestContext requestContext, EntityQueryRequest request) throws ConversionException {
    final CloseableIterator<Document> documentIterator;
    if (queryAggregationEnabled) {
      final Converter<EntityQueryRequest, org.hypertrace.core.documentstore.query.Query>
          queryConverter = getQueryConverter();
      final org.hypertrace.core.documentstore.query.Query query;

      query = queryConverter.convert(request, requestContext);
      documentIterator = entitiesCollection.aggregate(query);
    } else {
      // TODO: Optimize this later. For now converting to EDS Query and then again to DocStore
      // Query.
      Query query = entityQueryConverter.convertToEDSQuery(requestContext, request);
      /**
       * {@link EntityQueryRequest} selections need to treated differently, since they don't
       * transform one to one to {@link org.hypertrace.entity.data.service.v1.EntityDataRequest}
       * selections
       */
      List<String> docStoreSelections =
          entityQueryConverter.convertSelectionsToDocStoreSelections(
              requestContext, request.getSelectionList());
      documentIterator =
          entitiesCollection.search(
              DocStoreConverter.transform(
                  requestContext.getTenantId().orElseThrow(), query, docStoreSelections));
    }
    return documentIterator;
  }

  private ResultSetChunk convertDocumentsToResultSetChunk(
      final List<Document> documents, final List<Expression> selections)
      throws ConversionException {
    final DocumentConverter documentConverter = injector.getInstance(DocumentConverter.class);
    final ResultSetMetadata resultSetMetadata = this.buildMetadataForSelections(selections);

    ResultSetChunk.Builder resultBuilder = ResultSetChunk.newBuilder();
    // Build metadata
    resultBuilder.setResultSetMetadata(resultSetMetadata);
    // Build data
    for (final Document document : documents) {
      final Row row = documentConverter.convertToRow(document, resultSetMetadata);
      resultBuilder.addRow(row);
    }

    return resultBuilder.build();
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

      // Finally, return the selections
      List<Document> documents =
          getProjectedDocuments(
              request.getEntityIdsList(), request.getSelectionList(), requestContext);

      responseObserver.onNext(
          convertDocumentsToResultSetChunk(documents, request.getSelectionList()));
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(
          new ServiceException("Error occurred while executing " + request, e));
    }
  }

  private void doUpdate(RequestContext requestContext, EntityUpdateRequest request)
      throws Exception {
    Optional<String> maybeTenantId = requestContext.getTenantId();
    if (maybeTenantId.isEmpty()) {
      return;
    }

    String tenantId = maybeTenantId.get();
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
      Set<String> entityIdsForChangeNotification = new HashSet<>();
      boolean shouldSendNotification =
          this.entityAttributeChangeEvaluator.shouldSendNotification(
              requestContext, request.getEntityType(), request.getOperation());

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
        if (shouldSendNotification) {
          entityIdsForChangeNotification.add(entityId);
        }
      }
      try {
        List<Entity> existingEntities =
            this.entityFetcher.getEntitiesByEntityIds(tenantId, entityIdsForChangeNotification);
        entitiesCollection.bulkUpdateSubDocs(entitiesUpdateMap);

        List<Entity> updatedEntities =
            this.entityFetcher.getEntitiesByEntityIds(tenantId, entityIdsForChangeNotification);

        this.entityCounterMetricSender.sendEntitiesMetrics(
            requestContext, request.getEntityType(), existingEntities, updatedEntities);
        this.entityChangeEventGenerator.sendChangeNotification(
            requestContext, existingEntities, updatedEntities);
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
      doBulkUpdate(requestContext, request.getEntityType(), entitiesMap);
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(
          new ServiceException("Error occurred while executing " + request, e));
    }
  }

  @Override
  public void bulkUpdateEntityArrayAttribute(
      BulkEntityArrayAttributeUpdateRequest request,
      StreamObserver<BulkEntityArrayAttributeUpdateResponse> responseObserver) {
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
              .collect(Collectors.toCollection(LinkedHashSet::new));

      String attributeId = request.getAttribute().getColumnName();

      String subDocPath =
          entityAttributeMapping
              .getDocStorePathByAttributeId(requestContext, attributeId)
              .orElseThrow(() -> new IllegalArgumentException("Unknown attribute " + attributeId));

      List<Document> subDocuments =
          request.getValuesList().stream()
              .map(this::convertToJsonDocument)
              .collect(toUnmodifiableList());
      BulkArrayValueUpdateRequest bulkArrayValueUpdateRequest =
          new BulkArrayValueUpdateRequest(
              keys,
              subDocPath + ARRAY_VALUE_PATH_SUFFIX,
              getMatchingOperation(request.getOperation()),
              subDocuments);

      List<String> entityIdsForChangeNotifications = Lists.newArrayList();
      boolean shouldSendNotification =
          this.entityAttributeChangeEvaluator.shouldSendNotification(
              requestContext, request.getEntityType(), request.getAttribute());
      if (shouldSendNotification) {
        entityIdsForChangeNotifications = request.getEntityIdsList();
      }

      List<Entity> existingEntities =
          this.entityFetcher.getEntitiesByEntityIds(tenantId, entityIdsForChangeNotifications);
      entitiesCollection.bulkOperationOnArrayValue(bulkArrayValueUpdateRequest);

      List<Entity> updatedEntities =
          this.entityFetcher.getEntitiesByEntityIds(tenantId, request.getEntityIdsList());

      this.entityCounterMetricSender.sendEntitiesMetrics(
          requestContext, request.getEntityType(), existingEntities, updatedEntities);
      this.entityChangeEventGenerator.sendChangeNotification(
          requestContext, existingEntities, updatedEntities);

      responseObserver.onNext(BulkEntityArrayAttributeUpdateResponse.newBuilder().build());
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

  @SneakyThrows
  private JSONDocument convertToJsonDocument(LiteralConstant literalConstant) {
    // Convert setAttribute LiteralConstant to AttributeValue. Need to be able to store an array
    // literal constant as an array
    AttributeValue attributeValue =
        EntityQueryConverter.convertToAttributeValue(literalConstant).build();
    String jsonValue = PRINTER.print(attributeValue);
    return new JSONDocument(jsonValue);
  }

  private List<Document> getProjectedDocuments(
      final Iterable<String> entityIds,
      final List<Expression> selectionList,
      final RequestContext requestContext)
      throws ConversionException {
    final List<String> entityIdList = newArrayList(entityIds);

    if (entityIdList.isEmpty()) {
      return emptyList();
    }

    final Converter<List<Expression>, Selection> selectionConverter = getSelectionConverter();
    final Selection selection = selectionConverter.convert(selectionList, requestContext);
    final Filter filter =
        Filter.builder()
            .expression(
                RelationalExpression.of(
                    IdentifierExpression.of(EntityServiceConstants.ENTITY_ID),
                    IN,
                    ConstantExpression.ofStrings(entityIdList)))
            .build();

    final org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .setSelection(selection)
            .setFilter(filter)
            .build();
    final Iterator<Document> documentIterator = entitiesCollection.find(query);

    return newArrayList(documentIterator);
  }

  private void doBulkUpdate(
      RequestContext requestContext, String entityType, Map<String, EntityUpdateInfo> entitiesMap)
      throws Exception {
    Optional<String> maybeTenantId = requestContext.getTenantId();
    if (maybeTenantId.isEmpty()) {
      return;
    }

    String tenantId = maybeTenantId.get();
    Map<Key, Map<String, Document>> entitiesUpdateMap = new HashMap<>();
    Set<String> entityIdsForChangeNotification = new HashSet<>();
    for (String entityId : entitiesMap.keySet()) {
      List<UpdateOperation> updateOperations = entitiesMap.get(entityId).getUpdateOperationList();
      Map<String, Document> transformedUpdateOperations =
          transformUpdateOperations(updateOperations, requestContext);
      if (transformedUpdateOperations.isEmpty()) {
        continue;
      }
      entitiesUpdateMap.put(
          new SingleValueKey(requestContext.getTenantId().orElseThrow(), entityId),
          transformedUpdateOperations);
      boolean shouldSendNotification =
          this.entityAttributeChangeEvaluator.shouldSendNotification(
              requestContext, entityType, updateOperations);
      if (shouldSendNotification) {
        entityIdsForChangeNotification.add(entityId);
      }
    }

    if (entitiesUpdateMap.isEmpty()) {
      LOG.error("There are no entities to update!");
      return;
    }

    try {
      List<Entity> existingEntities =
          this.entityFetcher.getEntitiesByEntityIds(tenantId, entityIdsForChangeNotification);
      entitiesCollection.bulkUpdateSubDocs(entitiesUpdateMap);
      List<Entity> updatedEntities =
          this.entityFetcher.getEntitiesByEntityIds(tenantId, entityIdsForChangeNotification);

      this.entityCounterMetricSender.sendEntitiesMetrics(
          requestContext, entityType, existingEntities, updatedEntities);
      this.entityChangeEventGenerator.sendChangeNotification(
          requestContext, existingEntities, updatedEntities);
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

    // TODO: Replace to use the new org.hypertrace.core.documentstore.query.Query DTO
    long total =
        entitiesCollection.total(DocStoreConverter.transform(tenantId.get(), query, emptyList()));
    responseObserver.onNext(TotalEntitiesResponse.newBuilder().setTotal(total).build());
    responseObserver.onCompleted();
  }

  @Override
  public void deleteEntities(
      DeleteEntitiesRequest request, StreamObserver<DeleteEntitiesResponse> responseObserver) {
    RequestContext requestContext = RequestContext.CURRENT.get();
    List<Entity> existingEntities;
    try {
      validateDeleteEntitiesRequest(request, requestContext);
      existingEntities = getEntitiesToDelete(requestContext, request);
    } catch (Exception ex) {
      responseObserver.onError(ex);
      return;
    }

    if (existingEntities.size() == 0) {
      LOG.debug("{}. No entities found to delete", request);
      responseObserver.onNext(DeleteEntitiesResponse.newBuilder().build());
      responseObserver.onCompleted();
      return;
    }

    if (existingEntities.size() > maxEntitiesToDelete) {
      LOG.warn(
          "{}. Number of entity ids to delete exceeds the maximum limit: Entity Ids limit exceeded",
          request);
      responseObserver.onError(
          Status.FAILED_PRECONDITION
              .withDescription("Number of entity ids to delete exceeds the maximum limit.")
              .asRuntimeException());
      return;
    }

    List<String> entityIds =
        existingEntities.stream().map(Entity::getEntityId).collect(toUnmodifiableList());

    LOG.debug("Deleting entity of type: {} and ids: {}", request.getEntityType(), entityIds);
    try {
      Optional<String> tenantId = requestContext.getTenantId();
      this.entitiesCollection.delete(
          DocStoreConverter.transform(tenantId.orElseThrow(), request.getEntityType(), entityIds));

      this.entityCounterMetricSender.sendEntitiesDeleteMetrics(
          requestContext, request.getEntityType(), existingEntities);
      this.entityChangeEventGenerator.sendDeleteNotification(requestContext, existingEntities);

      responseObserver.onNext(
          DeleteEntitiesResponse.newBuilder().addAllEntityIds(entityIds).build());
      responseObserver.onCompleted();
    } catch (Exception ex) {
      LOG.error("Error while deleting entity ids {}", entityIds, ex);
      responseObserver.onError(
          Status.INTERNAL.withDescription("Error while deleting entity ids").asRuntimeException());
    }
  }

  @Override
  public void bulkUpdateAllMatchingFilter(
      final BulkUpdateAllMatchingFilterRequest request,
      final StreamObserver<BulkUpdateAllMatchingFilterResponse> responseObserver) {
    final RequestContext requestContext = RequestContext.CURRENT.get();
    final Optional<String> tenantIdOptional = requestContext.getTenantId();
    if (tenantIdOptional.isEmpty()) {
      LOG.warn("Tenant id is missing in bulk update entities request");
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription("Tenant id is missing in the request.")
              .asException());
      return;
    }

    if (StringUtils.isBlank(request.getEntityType())) {
      LOG.warn("Entity type is missing in bulk update entities request");
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription("Entity type is missing in the request.")
              .asException());
      return;
    }

    if (request.getUpdatesCount() == 0) {
      LOG.warn("No operation specified in bulk update entities request");
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription("No operation is specified in the request.")
              .asException());
      return;
    }

    if (anyStringUpdateViolatesLengthConstraint(request)) {
      LOG.warn(
          String.format(
              "String update value exceeded %d characters", MAX_STRING_LENGTH_FOR_UPDATE));
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(
                  String.format(
                      "Update value too long (> %d characters)", MAX_STRING_LENGTH_FOR_UPDATE))
              .asException());
    }

    try {
      final BulkUpdateAllMatchingFilterResponse response = doBulkUpdate(request, requestContext);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (final Exception e) {
      LOG.error("Error while bulk updating entities for " + tenantIdOptional.orElse(""), e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Error while bulk updating entities")
              .asRuntimeException());
    }
  }

  private BulkUpdateAllMatchingFilterResponse doBulkUpdate(
      final BulkUpdateAllMatchingFilterRequest request, final RequestContext requestContext)
      throws ConversionException, IOException {
    final BulkUpdateAllMatchingFilterResponse.Builder responseBuilder =
        BulkUpdateAllMatchingFilterResponse.newBuilder();
    final String entityType = request.getEntityType();
    final String tenantId = requestContext.getTenantId().orElseThrow();

    for (final Update update : request.getUpdatesList()) {
      final List<SingleValueKey> keys = getKeysToUpdate(requestContext, entityType, update);
      final List<UpdatedEntity> updatedEntityResponses = buildUpdatedEntities(keys);
      responseBuilder.addSummaries(
          UpdateSummary.newBuilder().addAllUpdatedEntities(updatedEntityResponses));

      if (keys.isEmpty()) {
        // Nothing to update
        LOG.info("No entity found with filter {} for updating", update.getFilter());
        continue;
      }

      final List<String> entityIds =
          keys.stream().map(SingleValueKey::getValue).collect(toUnmodifiableList());
      final List<AttributeUpdateOperation> updateOperations = update.getOperationsList();

      final FilterTypeExpression filter = getFilterForKeys(keys);

      final org.hypertrace.core.documentstore.query.Query updateQuery =
          org.hypertrace.core.documentstore.query.Query.builder().setFilter(filter).build();
      final List<SubDocumentUpdate> updates = convertUpdates(requestContext, updateOperations);

      final boolean shouldSendNotification =
          entityAttributeChangeEvaluator.shouldSendNotificationForAttributeUpdates(
              requestContext, entityType, updateOperations);
      final Optional<List<Entity>> existingEntities;

      if (shouldSendNotification) {
        existingEntities = Optional.of(entityFetcher.getEntitiesByEntityIds(tenantId, entityIds));
      } else {
        existingEntities = Optional.empty();
      }

      entitiesCollection.bulkUpdate(
          updateQuery, updates, UpdateOptions.builder().returnDocumentType(NONE).build());

      if (existingEntities.isPresent()) {
        final List<Entity> updatedEntities =
            entityFetcher.getEntitiesByEntityIds(tenantId, entityIds);
        this.entityCounterMetricSender.sendEntitiesMetrics(
            requestContext, request.getEntityType(), existingEntities.get(), updatedEntities);
        entityChangeEventGenerator.sendChangeNotification(
            requestContext, existingEntities.get(), updatedEntities);
      }
    }

    return responseBuilder.build();
  }

  private FilterTypeExpression getFilterForKeys(final List<SingleValueKey> keys) {
    final FilterTypeExpression filter;

    if (keys.size() == 1) {
      filter = KeyExpression.of(keys.get(0));
    } else {
      filter = or(keys.stream().map(KeyExpression::of).collect(toUnmodifiableList()));
    }
    return filter;
  }

  private List<UpdatedEntity> buildUpdatedEntities(final List<SingleValueKey> keys) {
    return keys.stream()
        .map(SingleValueKey::getValue)
        .map(id -> UpdatedEntity.newBuilder().setId(id))
        .map(UpdatedEntity.Builder::build)
        .collect(toUnmodifiableList());
  }

  @SuppressWarnings("Convert2Diamond")
  private Converter<AttributeUpdateOperation, SubDocumentUpdate> getUpdateConverter() {
    return injector.getInstance(
        com.google.inject.Key.get(
            new TypeLiteral<Converter<AttributeUpdateOperation, SubDocumentUpdate>>() {}));
  }

  private List<SingleValueKey> getKeysToUpdate(
      final RequestContext requestContext, final String entityType, final Update update)
      throws ConversionException, IOException {
    final Optional<String> idAttribute =
        entityAttributeMapping.getIdentifierAttributeId(entityType);

    if (idAttribute.isEmpty()) {
      throw Status.UNIMPLEMENTED
          .withDescription(String.format("Bulk updating %s entities is not supported", entityType))
          .asRuntimeException();
    }

    final Expression idSelection =
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName(idAttribute.orElseThrow()))
            .build();
    final EntityQueryRequest entityQueryRequest =
        EntityQueryRequest.newBuilder()
            .setEntityType(entityType)
            .setFilter(update.getFilter())
            .addSelection(idSelection)
            .build();
    final Converter<EntityQueryRequest, org.hypertrace.core.documentstore.query.Query>
        queryConverter = getQueryConverter();

    final org.hypertrace.core.documentstore.query.Query query =
        queryConverter.convert(entityQueryRequest, requestContext);

    final CloseableIterator<Document> idsIterator = entitiesCollection.aggregate(query);
    final DocumentConverter rowConverter = injector.getInstance(DocumentConverter.class);
    final ResultSetMetadata resultSetMetadata =
        this.buildMetadataForSelections(List.of(idSelection));
    final String tenantId = requestContext.getTenantId().orElseThrow();
    final List<SingleValueKey> ids = new ArrayList<>();

    while (idsIterator.hasNext()) {
      final Row row = rowConverter.convertToRow(idsIterator.next(), resultSetMetadata);
      final String id = row.getColumn(0).getString();
      ids.add(new SingleValueKey(tenantId, id));
    }

    idsIterator.close();
    return unmodifiableList(ids);
  }

  private List<SubDocumentUpdate> convertUpdates(
      final RequestContext requestContext, final List<AttributeUpdateOperation> updateOperations)
      throws ConversionException {
    final List<SubDocumentUpdate> updates = new ArrayList<>();
    final Converter<AttributeUpdateOperation, SubDocumentUpdate> updateConverter =
        getUpdateConverter();

    for (final AttributeUpdateOperation operation : updateOperations) {
      final SubDocumentUpdate convert = updateConverter.convert(operation, requestContext);
      updates.add(convert);
    }
    return unmodifiableList(updates);
  }

  private List<Entity> getEntitiesToDelete(
      RequestContext requestContext, DeleteEntitiesRequest request) {
    try {
      EntityQueryRequest entityQueryRequest =
          EntityQueryRequest.newBuilder()
              .setEntityType(request.getEntityType())
              .setFilter(request.getFilter())
              .setLimit(maxEntitiesToDelete + 1)
              .build();

      final Converter<EntityQueryRequest, org.hypertrace.core.documentstore.query.Query>
          queryConverter = getQueryConverter();
      final org.hypertrace.core.documentstore.query.Query query;
      query = queryConverter.convert(entityQueryRequest, requestContext);
      return this.entityFetcher.query(query).collect(toUnmodifiableList());
    } catch (Exception ex) {
      LOG.error("Error while getting entity ids to delete", ex);
      throw Status.INVALID_ARGUMENT
          .withDescription("Error while getting entity ids to delete")
          .asRuntimeException();
    }
  }

  @Deprecated(
      since =
          "Will be removed when Collection.find() and Collection.aggregate() are implemented for "
              + "Postgres and the 'queryAggregationEnabled' helm-value is enabled",
      forRemoval = true)
  private Row convertToEntityQueryResult(
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
                          .setValueType(STRING)
                          .setString(entity.getEntityId())
                          .build());
                } else if (edsSubDocPath.equals(EntityServiceConstants.ENTITY_NAME)) {
                  result.addColumn(
                      Value.newBuilder()
                          .setValueType(STRING)
                          .setString(entity.getEntityName())
                          .build());
                } else if (edsSubDocPath.equals(EntityServiceConstants.ENTITY_CREATED_TIME)) {
                  result.addColumn(
                      Value.newBuilder()
                          .setValueType(ValueType.LONG)
                          .setLong(entity.getCreatedTime())
                          .build());
                } else if (edsSubDocPath.startsWith(ENTITY_ATTRIBUTE_DOC_PREFIX)) {
                  // Convert EDS AttributeValue to Gateway Value
                  AttributeValue attributeValue =
                      entity.getAttributesMap().get(edsSubDocPath.split("\\.")[1]);
                  result.addColumn(
                      EntityQueryConverter.convertAttributeValueToQueryValue(attributeValue));
                } else {
                  LOG.error(
                      "Unable to add column {} for sub doc path {}", columnName, edsSubDocPath);
                }
              } else {
                LOG.warn("columnName {} missing in attrNameToEDSAttrMap", columnName);
                result.addColumn(Value.getDefaultInstance());
              }
            });
    return result.build();
  }

  private ResultSetMetadata buildMetadataForSelections(List<Expression> selections)
      throws ConversionException {
    final AliasProvider<ColumnIdentifier> identifierAliasProvider = getIdentifierAliasProvider();
    final AliasProvider<Function> aggregateExpressionAliasProvider =
        getAggregateExpressionAliasProvider();

    final List<ColumnMetadata> list = new ArrayList<>();

    for (final Expression selection : selections) {
      final String columnName;

      if (selection.hasFunction()) {
        columnName = aggregateExpressionAliasProvider.getAlias(selection.getFunction());
      } else if (selection.hasColumnIdentifier()) {
        columnName = identifierAliasProvider.getAlias(selection.getColumnIdentifier());
      } else {
        throw new ConversionException(
            String.format(
                "Selection of non-identifier and non-aggregation is not supported. Found: %s",
                selection));
      }

      ColumnMetadata build = ColumnMetadata.newBuilder().setColumnName(columnName).build();
      list.add(build);
    }

    return ResultSetMetadata.newBuilder().addAllColumnMetadata(list).build();
  }

  @SuppressWarnings("Convert2Diamond")
  private AliasProvider<ColumnIdentifier> getIdentifierAliasProvider() {
    return injector.getInstance(
        com.google.inject.Key.get(new TypeLiteral<AliasProvider<ColumnIdentifier>>() {}));
  }

  @SuppressWarnings("Convert2Diamond")
  private AliasProvider<Function> getAggregateExpressionAliasProvider() {
    return injector.getInstance(
        com.google.inject.Key.get(new TypeLiteral<AliasProvider<Function>>() {}));
  }

  @SuppressWarnings("Convert2Diamond")
  private Converter<List<Expression>, Selection> getSelectionConverter() {
    return injector.getInstance(
        com.google.inject.Key.get(new TypeLiteral<Converter<List<Expression>, Selection>>() {}));
  }

  @SuppressWarnings("Convert2Diamond")
  private Converter<EntityQueryRequest, org.hypertrace.core.documentstore.query.Query>
      getQueryConverter() {
    return injector.getInstance(
        com.google.inject.Key.get(
            new TypeLiteral<
                Converter<
                    EntityQueryRequest, org.hypertrace.core.documentstore.query.Query>>() {}));
  }

  private void validateDeleteEntitiesRequest(
      DeleteEntitiesRequest request, RequestContext requestContext) {
    Optional<String> tenantId = requestContext.getTenantId();
    if (tenantId.isEmpty()) {
      LOG.error("{}. Invalid deleteEntities request: Tenant id is not provided", request);
      throw Status.INVALID_ARGUMENT
          .withDescription("Invalid deleteEntities request: Tenant id is not provided")
          .asRuntimeException();
    }

    if (request.getEntityType().isEmpty()) {
      LOG.error("{}. Invalid deleteEntities request: Entity Type is empty", request);
      throw Status.INVALID_ARGUMENT
          .withDescription("Invalid deleteEntities request: Entity Type is empty")
          .asRuntimeException();
    }
  }

  private boolean anyStringUpdateViolatesLengthConstraint(
      final BulkUpdateAllMatchingFilterRequest request) {
    return request.getUpdatesList().stream()
        .map(Update::getOperationsList)
        .flatMap(List::stream)
        .map(AttributeUpdateOperation::getValue)
        .map(LiteralConstant::getValue)
        .flatMap(this::getStringStream)
        .map(String::length)
        .anyMatch(length -> length > MAX_STRING_LENGTH_FOR_UPDATE);
  }

  private Stream<String> getStringStream(final Value value) {
    switch (value.getValueType()) {
      case STRING:
        return Stream.of(value.getString());
      case STRING_ARRAY:
        return value.getStringArrayList().stream();
      default:
        return Stream.of();
    }
  }
}
