package org.hypertrace.entity.query.service;

import static java.util.stream.Collectors.toUnmodifiableMap;
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
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.DocumentParser;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Query;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.ColumnMetadata;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc.EntityQueryServiceImplBase;
import org.hypertrace.entity.query.service.v1.EntityUpdateRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.ResultSetMetadata;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.SetAttribute;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.service.constants.EntityServiceConstants;
import org.hypertrace.entity.service.util.DocStoreConverter;
import org.hypertrace.entity.service.util.DocStoreJsonFormat;
import org.hypertrace.entity.service.util.DocStoreJsonFormat.Parser;
import org.hypertrace.entity.service.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityQueryServiceImpl extends EntityQueryServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(EntityQueryServiceImpl.class);
  private static final String ATTRIBUTE_MAP_CONFIG_PATH = "entity.service.attributeMap";
  private static final Parser PARSER = DocStoreJsonFormat.parser().ignoringUnknownFields();
  private static final DocumentParser DOCUMENT_PARSER = new DocumentParser();
  private static final String CHUNK_SIZE_CONFIG = "entity.query.service.response.chunk.size";
  private static final int DEFAULT_CHUNK_SIZE = 10_000;

  private final Collection entitiesCollection;
  private final Map<String, Map<String, String>> attrNameToEDSAttrMap;
  private final int CHUNK_SIZE;

  public EntityQueryServiceImpl(Datastore datastore, Config config) {
    this(
        datastore.getCollection(RAW_ENTITIES_COLLECTION),
        config.getConfigList(ATTRIBUTE_MAP_CONFIG_PATH)
            .stream()
            .collect(toUnmodifiableMap(
                conf -> conf.getString("scope"),
                conf -> Map.of(conf.getString("name"), conf.getString("subDocPath")),
                (map1, map2) -> {
                  Map<String, String> map = new HashMap<>();
                  map.putAll(map1);
                  map.putAll(map2);
                  return map;
                }
            )), !config.hasPathOrNull(CHUNK_SIZE_CONFIG) ? DEFAULT_CHUNK_SIZE : config.getInt(CHUNK_SIZE_CONFIG));
  }

  public EntityQueryServiceImpl(
      Collection entitiesCollection,
      Map<String, Map<String, String>> attrNameToEDSAttrMap,
      int chunkSize) {
    this.entitiesCollection = entitiesCollection;
    this.attrNameToEDSAttrMap = attrNameToEDSAttrMap;
    this.CHUNK_SIZE = chunkSize;
  }

  @Override
  public void execute(EntityQueryRequest request, StreamObserver<ResultSetChunk> responseObserver) {
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    //TODO: Optimize this later. For now converting to EDS Query and then again to DocStore Query.
    Query query = EntityQueryConverter
        .convertToEDSQuery(request, attrNameToEDSAttrMap.get(request.getEntityType()));
    /**
     * {@link EntityQueryRequest} selections need to treated differently, since they don't transform
     * one to one to {@link org.hypertrace.entity.data.service.v1.EntityDataRequest} selections
     */
    Map<String, String> scopedAttrNameToEDSAttrMap = attrNameToEDSAttrMap.get(request.getEntityType());
    List<String> docStoreSelections =
        EntityQueryConverter.convertSelectionsToDocStoreSelections(
            request.getSelectionList(), scopedAttrNameToEDSAttrMap);
    Iterator<Document> documentIterator = entitiesCollection.search(
        DocStoreConverter.transform(tenantId.get(), query, docStoreSelections));

    ResultSetMetadata resultSetMetadata = ResultSetMetadata.newBuilder()
        .addAllColumnMetadata(
            () -> request.getSelectionList().stream().map(Expression::getColumnIdentifier)
                .map(
                    ColumnIdentifier::getColumnName)
                .map(s -> ColumnMetadata.newBuilder().setColumnName(s).build()).iterator())
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
      Optional<Entity> entity = DOCUMENT_PARSER.parseOrLog(documentIterator.next(), Entity.newBuilder());
      // Set metadata for new chunk
      if (isNewChunk) {
        resultBuilder.setResultSetMetadata(resultSetMetadata);
        isNewChunk = false;
      }
      if (entity.isPresent()) {
        //Build data
        resultBuilder.addRow(convertToEntityQueryResult(
            entity.get(),
            request.getSelectionList(),
            attrNameToEDSAttrMap.get(request.getEntityType())));
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

  private static ResultSetChunk convertEntitiesToResultSetChunk(
      List<Entity> entities,
      List<Expression> selections,
      Map<String, String> attributeFqnMapping) {

    ResultSetChunk.Builder resultBuilder = ResultSetChunk.newBuilder();
    //Build metadata
    resultBuilder.setResultSetMetadata(ResultSetMetadata.newBuilder()
        .addAllColumnMetadata(
            () -> selections.stream().map(Expression::getColumnIdentifier).map(
                ColumnIdentifier::getColumnName)
                .map(s -> ColumnMetadata.newBuilder().setColumnName(s).build()).iterator())
        .build());
    //Build data
    resultBuilder.addAllRow(() -> entities.stream().map(
        entity -> convertToEntityQueryResult(entity, selections, attributeFqnMapping)).iterator());

    return resultBuilder.build();
  }

  static Row convertToEntityQueryResult(
      Entity entity, List<Expression> selections, Map<String, String> egsToEdsAttrMapping) {
    Row.Builder result = Row.newBuilder();
    selections.stream()
        .filter(expression -> expression.getValueCase() == ValueCase.COLUMNIDENTIFIER)
        .forEach(expression -> {
          String columnName = expression.getColumnIdentifier().getColumnName();
          String edsSubDocPath = egsToEdsAttrMapping.get(columnName);
          if (edsSubDocPath != null) {
            //Map the attr name to corresponding Attribute Key in EDS and get the EDS AttributeValue
            if (edsSubDocPath.equals(EntityServiceConstants.ENTITY_ID)) {
              result.addColumn(Value.newBuilder()
                  .setValueType(ValueType.STRING)
                  .setString(entity.getEntityId())
                  .build());
            } else if (edsSubDocPath.equals(EntityServiceConstants.ENTITY_NAME)) {
              result.addColumn(Value.newBuilder()
                  .setValueType(ValueType.STRING)
                  .setString(entity.getEntityName())
                  .build());
            } else if (edsSubDocPath.startsWith("attributes.")) {
              //Convert EDS AttributeValue to Gateway Value
              AttributeValue attributeValue = entity.getAttributesMap()
                  .get(edsSubDocPath.split("\\.")[1]);
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
    Optional<String> tenantId = RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
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
      Map<String, String> attributeFqnMap = attrNameToEDSAttrMap.get(request.getEntityType());
      doUpdate(request, attributeFqnMap, tenantId.get());

      // Finally return the selections
      Query entitiesQuery = Query.newBuilder().addAllEntityId(request.getEntityIdsList()).build();
      List<String> docStoreSelections =
          EntityQueryConverter.convertSelectionsToDocStoreSelections(
              request.getSelectionList(), attributeFqnMap);
      Iterator<Document> documentIterator =
          entitiesCollection.search(
              DocStoreConverter.transform(tenantId.get(), entitiesQuery, docStoreSelections));
      List<Entity> entities = convertDocsToEntities(documentIterator);
      responseObserver.onNext(convertEntitiesToResultSetChunk(
          entities,
          request.getSelectionList(),
          attrNameToEDSAttrMap.get(request.getEntityType())));
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver
          .onError(new ServiceException("Error occurred while executing " + request, e));
    }
  }

  private void doUpdate(EntityUpdateRequest request, Map<String, String> attributeFqnMap,
      String tenantId) throws IOException {
    if (request.getOperation().hasSetAttribute()) {
      SetAttribute setAttribute = request.getOperation().getSetAttribute();
      String attributeFqn = setAttribute.getAttribute().getColumnName();
      if (!attributeFqnMap.containsKey(attributeFqn)) {
        throw new IllegalArgumentException("Unknown attribute FQN " + attributeFqn);
      }
      String subDocPath = attributeFqnMap.get(attributeFqn);
      // Convert setAttribute LiteralConstant to AttributeValue. Need to be able to store an array
      // literal constant as an array
      AttributeValue attributeValue = EntityQueryConverter.convertToAttributeValue(setAttribute.getValue()).build();
      String jsonValue = DocStoreJsonFormat.printer().print(attributeValue);

      for (String entityId : request.getEntityIdsList()) {
        SingleValueKey key = new SingleValueKey(tenantId, entityId);
        // TODO better error reporting once doc store exposes the,
        if (!entitiesCollection.updateSubDoc(
            key, subDocPath, new JSONDocument(jsonValue))) {
          LOG.warn("Failed to update entity {}, subDocPath {}, with new doc {}.", key, subDocPath,
              jsonValue);
        }
      }
    }
  }
}
