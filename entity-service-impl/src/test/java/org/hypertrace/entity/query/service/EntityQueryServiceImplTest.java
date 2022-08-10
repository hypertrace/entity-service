package org.hypertrace.entity.query.service;

import static org.hypertrace.entity.TestUtils.convertToCloseableIterator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import com.google.common.collect.Streams;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest;
import org.hypertrace.core.documentstore.BulkUpdateResult;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.fetcher.EntityFetcher;
import org.hypertrace.entity.query.service.v1.BulkEntityArrayAttributeUpdateRequest;
import org.hypertrace.entity.query.service.v1.BulkEntityArrayAttributeUpdateResponse;
import org.hypertrace.entity.query.service.v1.BulkEntityUpdateRequest;
import org.hypertrace.entity.query.service.v1.BulkEntityUpdateRequest.EntityUpdateInfo;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.ColumnMetadata;
import org.hypertrace.entity.query.service.v1.DeleteEntitiesRequest;
import org.hypertrace.entity.query.service.v1.DeleteEntitiesResponse;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.EntityUpdateRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.LiteralConstant.Builder;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.OrderByExpression;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.ResultSetMetadata;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.SetAttribute;
import org.hypertrace.entity.query.service.v1.TotalEntitiesRequest;
import org.hypertrace.entity.query.service.v1.TotalEntitiesResponse;
import org.hypertrace.entity.query.service.v1.UpdateOperation;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.service.change.event.api.EntityChangeEventGenerator;
import org.hypertrace.entity.service.util.DocStoreJsonFormat;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
public class EntityQueryServiceImplTest {

  private static final String TEST_ENTITY_TYPE = "TEST_ENTITY";
  @Mock RequestContext requestContext;
  @Mock EntityAttributeMapping mockAttributeMapping;
  @Mock Collection entitiesCollection;
  @Mock EntityChangeEventGenerator entityChangeEventGenerator;
  @Mock EntityFetcher entityFetcher;

  private static final String API_ID = "API.id";
  private static final String ATTRIBUTE_ID1 = "Entity.id";
  private static final String EDS_API_ID_COLUMN_NAME = "entityId";
  private static final String EDS_COLUMN_NAME1 = "attributes.entity_id";
  private static final String ATTRIBUTE_ID2 = "Entity.status";
  private static final String EDS_COLUMN_NAME2 = "attributes.status";
  private static final String ATTRIBUTE_ID3 = "Entity.labels";
  private static final String EDS_COLUMN_NAME3 = "attributes.labels";

  @Test
  public void testUpdate_noTenantId() throws Exception {
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);
    when(requestContext.getTenantId()).thenReturn(Optional.empty());
    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(
                      entitiesCollection,
                      mockAttributeMapping,
                      entityChangeEventGenerator,
                      entityFetcher,
                      1,
                      false,
                      1000);

              eqs.update(null, mockResponseObserver);

              verify(mockResponseObserver, times(1))
                  .onError(
                      argThat(new ExceptionMessageMatcher("Tenant id is missing in the request.")));
              return null;
            });
  }

  @Test
  public void testUpdate_noEntityType() throws Exception {
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);

    Context.current()
        .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(
                      entitiesCollection,
                      mockAttributeMapping,
                      entityChangeEventGenerator,
                      entityFetcher,
                      1,
                      false,
                      1000);

              eqs.update(EntityUpdateRequest.newBuilder().build(), mockResponseObserver);

              verify(mockResponseObserver, times(1))
                  .onError(
                      argThat(
                          new ExceptionMessageMatcher("Entity type is missing in the request.")));
              return null;
            });
  }

  @Test
  public void testUpdate_noEntityId() throws Exception {
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);

    Context.current()
        .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(
                      entitiesCollection,
                      mockAttributeMapping,
                      entityChangeEventGenerator,
                      entityFetcher,
                      1,
                      false,
                      1000);

              eqs.update(
                  EntityUpdateRequest.newBuilder().setEntityType(TEST_ENTITY_TYPE).build(),
                  mockResponseObserver);

              verify(mockResponseObserver, times(1))
                  .onError(
                      argThat(
                          new ExceptionMessageMatcher("Entity IDs are missing in the request.")));
              return null;
            });
  }

  @Test
  public void testUpdate_noOperation() throws Exception {
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);

    Context.current()
        .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(
                      entitiesCollection,
                      mockAttributeMapping,
                      entityChangeEventGenerator,
                      entityFetcher,
                      1,
                      false,
                      1000);

              eqs.update(
                  EntityUpdateRequest.newBuilder()
                      .setEntityType(TEST_ENTITY_TYPE)
                      .addEntityIds("entity-id-1")
                      .build(),
                  mockResponseObserver);

              verify(mockResponseObserver, times(1))
                  .onError(
                      argThat(new ExceptionMessageMatcher("Operation is missing in the request.")));
              return null;
            });
  }

  @Test
  public void testUpdate_success() throws Exception {
    Collection mockEntitiesCollection = mockEntitiesCollection();

    Builder newStatus =
        LiteralConstant.newBuilder()
            .setValue(Value.newBuilder().setValueType(ValueType.STRING).setString("NEW_STATUS"));

    EntityUpdateRequest updateRequest =
        EntityUpdateRequest.newBuilder()
            .setEntityType(TEST_ENTITY_TYPE)
            .addEntityIds("entity-id-1")
            .setOperation(
                UpdateOperation.newBuilder()
                    .setSetAttribute(
                        SetAttribute.newBuilder()
                            .setAttribute(
                                ColumnIdentifier.newBuilder().setColumnName(ATTRIBUTE_ID2))
                            .setValue(newStatus)))
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName(ATTRIBUTE_ID1)))
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName(ATTRIBUTE_ID2)))
            .build();

    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);

    Context.current()
        .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(
                      mockEntitiesCollection,
                      mockMappingForAttributes1And2(),
                      entityChangeEventGenerator,
                      entityFetcher,
                      1,
                      false,
                      1000);
              eqs.update(updateRequest, mockResponseObserver);
              return null;
            });

    verify(mockEntitiesCollection, times(1))
        .bulkUpdateSubDocs(
            eq(
                Map.of(
                    new SingleValueKey("tenant1", "entity-id-1"),
                    Map.of(
                        "attributes.status",
                        new JSONDocument(DocStoreJsonFormat.printer().print(newStatus))))));
  }

  @Nested
  class BulkUpdateEntities {

    @Test
    public void testBulkUpdate_noTenantId() throws Exception {
      StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);
      when(requestContext.getTenantId()).thenReturn(Optional.empty());
      Context.current()
          .withValue(RequestContext.CURRENT, requestContext)
          .call(
              () -> {
                EntityQueryServiceImpl eqs =
                    new EntityQueryServiceImpl(
                        entitiesCollection,
                        mockAttributeMapping,
                        entityChangeEventGenerator,
                        entityFetcher,
                        1,
                        false,
                        1000);

                eqs.bulkUpdate(null, mockResponseObserver);

                verify(mockResponseObserver, times(1))
                    .onError(
                        argThat(
                            new ExceptionMessageMatcher("Tenant id is missing in the request.")));
                return null;
              });
    }

    @Test
    public void testBulkUpdate_noEntityType() throws Exception {
      StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);

      Context.current()
          .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
          .call(
              () -> {
                EntityQueryServiceImpl eqs =
                    new EntityQueryServiceImpl(
                        entitiesCollection,
                        mockAttributeMapping,
                        entityChangeEventGenerator,
                        entityFetcher,
                        1,
                        false,
                        1000);

                eqs.bulkUpdate(BulkEntityUpdateRequest.newBuilder().build(), mockResponseObserver);

                verify(mockResponseObserver, times(1))
                    .onError(
                        argThat(
                            new ExceptionMessageMatcher("Entity type is missing in the request.")));
                return null;
              });
    }

    @Test
    public void testBulkUpdate_noEntities() throws Exception {
      StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);

      Context.current()
          .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
          .call(
              () -> {
                EntityQueryServiceImpl eqs =
                    new EntityQueryServiceImpl(
                        entitiesCollection,
                        mockAttributeMapping,
                        entityChangeEventGenerator,
                        entityFetcher,
                        1,
                        false,
                        1000);

                eqs.bulkUpdate(
                    BulkEntityUpdateRequest.newBuilder().setEntityType(TEST_ENTITY_TYPE).build(),
                    mockResponseObserver);

                verify(mockResponseObserver, times(1))
                    .onError(
                        argThat(
                            new ExceptionMessageMatcher("Entities are missing in the request.")));
                return null;
              });
    }

    @Test
    public void testBulkUpdate_entitiesWithNoUpdateOperations() throws Exception {
      EntityUpdateInfo.Builder updateInfo = EntityUpdateInfo.newBuilder();
      BulkEntityUpdateRequest bulkUpdateRequest =
          BulkEntityUpdateRequest.newBuilder()
              .setEntityType(TEST_ENTITY_TYPE)
              .putEntities("entity-id-1", updateInfo.build())
              .build();

      StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);

      Context.current()
          .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
          .call(
              () -> {
                EntityQueryServiceImpl eqs =
                    new EntityQueryServiceImpl(
                        entitiesCollection,
                        mockAttributeMapping,
                        entityChangeEventGenerator,
                        entityFetcher,
                        1,
                        false,
                        1000);
                eqs.bulkUpdate(bulkUpdateRequest, mockResponseObserver);
                return null;
              });
      verify(entitiesCollection, Mockito.never()).bulkUpdateSubDocs(any());
    }

    @Test
    public void testBulkUpdate_success() throws Exception {
      Collection mockEntitiesCollection = mockEntitiesCollection();

      Builder newStatus =
          LiteralConstant.newBuilder()
              .setValue(Value.newBuilder().setValueType(ValueType.STRING).setString("NEW_STATUS"));

      UpdateOperation.Builder updateOperation =
          UpdateOperation.newBuilder()
              .setSetAttribute(
                  SetAttribute.newBuilder()
                      .setAttribute(ColumnIdentifier.newBuilder().setColumnName(ATTRIBUTE_ID1))
                      .setValue(newStatus));
      EntityUpdateInfo.Builder updateInfo =
          EntityUpdateInfo.newBuilder().addUpdateOperation(updateOperation);
      BulkEntityUpdateRequest bulkUpdateRequest =
          BulkEntityUpdateRequest.newBuilder()
              .setEntityType(TEST_ENTITY_TYPE)
              .putEntities("entity-id-1", updateInfo.build())
              .build();

      StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);

      Context.current()
          .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
          .call(
              () -> {
                EntityQueryServiceImpl eqs =
                    new EntityQueryServiceImpl(
                        mockEntitiesCollection,
                        mockMappingForAttribute1(),
                        entityChangeEventGenerator,
                        entityFetcher,
                        1,
                        false,
                        1000);
                eqs.bulkUpdate(bulkUpdateRequest, mockResponseObserver);
                return null;
              });

      verify(mockEntitiesCollection, times(1))
          .bulkUpdateSubDocs(
              eq(
                  Map.of(
                      new SingleValueKey("tenant1", "entity-id-1"),
                      Map.of(
                          "attributes.entity_id",
                          new JSONDocument(DocStoreJsonFormat.printer().print(newStatus))))));
    }
  }

  @Test
  public void testExecute_noTenantId() throws Exception {
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);
    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(
                      entitiesCollection,
                      mockAttributeMapping,
                      entityChangeEventGenerator,
                      entityFetcher,
                      1,
                      false,
                      1000);

              eqs.execute(null, mockResponseObserver);

              verify(mockResponseObserver, times(1))
                  .onError(
                      argThat(new ExceptionMessageMatcher("Tenant id is missing in the request.")));
              return null;
            });
  }

  @Test
  public void testExecute_success() throws Exception {
    Collection mockEntitiesCollection = mock(Collection.class);
    Entity entity1 =
        Entity.newBuilder()
            .setTenantId("tenant-1")
            .setEntityType(TEST_ENTITY_TYPE)
            .setEntityId(UUID.randomUUID().toString())
            .setEntityName("Test entity 1")
            .putAttributes(
                EDS_COLUMN_NAME1,
                AttributeValue.newBuilder()
                    .setValue(
                        org.hypertrace.entity.data.service.v1.Value.newBuilder().setString("foo1"))
                    .build())
            .build();
    Entity entity2 =
        Entity.newBuilder()
            .setTenantId("tenant-1")
            .setEntityType(TEST_ENTITY_TYPE)
            .setEntityId(UUID.randomUUID().toString())
            .setEntityName("Test entity 2")
            .putAttributes(
                EDS_COLUMN_NAME1,
                AttributeValue.newBuilder()
                    .setValue(
                        org.hypertrace.entity.data.service.v1.Value.newBuilder().setString("foo2"))
                    .build())
            .build();

    List<Document> docs =
        List.of(
            new JSONDocument(JsonFormat.printer().print(entity1)),
            new JSONDocument(JsonFormat.printer().print(entity2)),
            // this doc will result in parsing error
            new JSONDocument("{\"entityId\": [1, 2]}"));
    when(mockEntitiesCollection.aggregate(any()))
        .thenReturn(convertToCloseableIterator(docs.iterator()));
    when(mockEntitiesCollection.search(any()))
        .thenReturn(convertToCloseableIterator(docs.iterator()));
    EntityQueryRequest request =
        EntityQueryRequest.newBuilder()
            .setEntityType(TEST_ENTITY_TYPE)
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setExpression(
                        Expression.newBuilder()
                            .setColumnIdentifier(
                                ColumnIdentifier.newBuilder()
                                    .setColumnName(ATTRIBUTE_ID1)
                                    .build())))
            .build();
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);
    Context.current()
        .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(
                      mockEntitiesCollection,
                      mockMappingForAttributes1And2(),
                      entityChangeEventGenerator,
                      1,
                      false,
                      1000);

              eqs.execute(request, mockResponseObserver);
              return null;
            });

    verify(mockEntitiesCollection, times(0)).aggregate(any());
    verify(mockEntitiesCollection, times(1)).search(any());
    verify(mockResponseObserver, times(3)).onNext(any());
    verify(mockResponseObserver, times(1)).onCompleted();
  }

  @Test
  public void testExecute_success_chunksize_2() throws Exception {
    Collection mockEntitiesCollection = mock(Collection.class);
    Entity entity1 =
        Entity.newBuilder()
            .setTenantId("tenant-1")
            .setEntityType(TEST_ENTITY_TYPE)
            .setEntityId(UUID.randomUUID().toString())
            .setEntityName("Test entity 1")
            .putAttributes(
                EDS_COLUMN_NAME1,
                AttributeValue.newBuilder()
                    .setValue(
                        org.hypertrace.entity.data.service.v1.Value.newBuilder().setString("foo1"))
                    .build())
            .build();
    Entity entity2 =
        Entity.newBuilder()
            .setTenantId("tenant-1")
            .setEntityType(TEST_ENTITY_TYPE)
            .setEntityId(UUID.randomUUID().toString())
            .setEntityName("Test entity 2")
            .putAttributes(
                EDS_COLUMN_NAME1,
                AttributeValue.newBuilder()
                    .setValue(
                        org.hypertrace.entity.data.service.v1.Value.newBuilder().setString("foo2"))
                    .build())
            .build();

    Entity entity3 =
        Entity.newBuilder()
            .setTenantId("tenant-1")
            .setEntityType(TEST_ENTITY_TYPE)
            .setEntityId(UUID.randomUUID().toString())
            .setEntityName("Test entity 3")
            .putAttributes(
                EDS_COLUMN_NAME1,
                AttributeValue.newBuilder()
                    .setValue(
                        org.hypertrace.entity.data.service.v1.Value.newBuilder().setString("foo2"))
                    .build())
            .build();

    List<Document> docs =
        List.of(
            new JSONDocument(JsonFormat.printer().print(entity1)),
            new JSONDocument(JsonFormat.printer().print(entity2)),
            new JSONDocument(JsonFormat.printer().print(entity3)),
            // this doc will result in parsing error
            new JSONDocument("{\"entityId\": [1, 2]}"));
    when(mockEntitiesCollection.aggregate(any()))
        .thenReturn(convertToCloseableIterator(docs.iterator()));
    when(mockEntitiesCollection.search(any()))
        .thenReturn(convertToCloseableIterator(docs.iterator()));

    EntityQueryRequest request =
        EntityQueryRequest.newBuilder()
            .setEntityType(TEST_ENTITY_TYPE)
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setExpression(
                        Expression.newBuilder()
                            .setColumnIdentifier(
                                ColumnIdentifier.newBuilder()
                                    .setColumnName(ATTRIBUTE_ID1)
                                    .build())))
            .build();
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);
    Context.current()
        .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(
                      mockEntitiesCollection,
                      mockMappingForAttributes1And2(),
                      entityChangeEventGenerator,
                      2,
                      false,
                      1000);

              eqs.execute(request, mockResponseObserver);
              return null;
            });

    verify(mockEntitiesCollection, times(0)).aggregate(any());
    verify(mockEntitiesCollection, times(1)).search(any());
    verify(mockResponseObserver, times(2)).onNext(any());
    verify(mockResponseObserver, times(1)).onCompleted();
  }

  @Test
  void testBulkUpdateEntityArrayAttribute() throws Exception {
    List<String> entityIds =
        IntStream.rangeClosed(1, 5)
            .mapToObj(i -> UUID.randomUUID().toString())
            .collect(Collectors.toList());
    BulkEntityArrayAttributeUpdateRequest request =
        BulkEntityArrayAttributeUpdateRequest.newBuilder()
            .addAllEntityIds(entityIds)
            .setAttribute(ColumnIdentifier.newBuilder().setColumnName(ATTRIBUTE_ID3).build())
            .setEntityType(TEST_ENTITY_TYPE)
            .setOperation(BulkEntityArrayAttributeUpdateRequest.Operation.OPERATION_ADD)
            .addAllValues(
                List.of(
                    LiteralConstant.newBuilder()
                        .setValue(Value.newBuilder().setString("Label1"))
                        .build(),
                    LiteralConstant.newBuilder()
                        .setValue(Value.newBuilder().setString("Label2"))
                        .build()))
            .build();

    when(mockAttributeMapping.getDocStorePathByAttributeId(requestContext, ATTRIBUTE_ID3))
        .thenReturn(Optional.of(EDS_COLUMN_NAME3));

    StreamObserver<BulkEntityArrayAttributeUpdateResponse> mockResponseObserver =
        mock(StreamObserver.class);

    Context.current()
        .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(
                      entitiesCollection,
                      mockAttributeMapping,
                      entityChangeEventGenerator,
                      entityFetcher,
                      1,
                      false,
                      1000);
              eqs.bulkUpdateEntityArrayAttribute(request, mockResponseObserver);
              return null;
            });

    ArgumentCaptor<BulkArrayValueUpdateRequest> argumentCaptor =
        ArgumentCaptor.forClass(BulkArrayValueUpdateRequest.class);
    verify(entitiesCollection, times(1)).bulkOperationOnArrayValue(argumentCaptor.capture());
    BulkArrayValueUpdateRequest bulkArrayValueUpdateRequest = argumentCaptor.getValue();
    assertEquals(
        entityIds.stream()
            .map(entityId -> new SingleValueKey("tenant1", entityId))
            .collect(Collectors.toCollection(LinkedHashSet::new)),
        bulkArrayValueUpdateRequest.getKeys());
    assertEquals(
        BulkArrayValueUpdateRequest.Operation.ADD, bulkArrayValueUpdateRequest.getOperation());
    assertEquals(
        EDS_COLUMN_NAME3 + ".valueList.values", bulkArrayValueUpdateRequest.getSubDocPath());
    List<Document> subDocuments = bulkArrayValueUpdateRequest.getSubDocuments();
    assertEquals(2, subDocuments.size());
    assertEquals("{\"value\":{\"string\":\"Label1\"}}", subDocuments.get(0).toString());
    assertEquals("{\"value\":{\"string\":\"Label2\"}}", subDocuments.get(1).toString());
  }

  @Test
  public void testDeleteEntities() {
    Collection mockEntitiesCollection = mock(Collection.class);
    UUID entityId = UUID.randomUUID();
    List<Entity> docs = List.of(Entity.newBuilder().setEntityId(entityId.toString()).build());

    when(this.entityFetcher.query(any(org.hypertrace.core.documentstore.query.Query.class)))
        .thenReturn(Streams.stream(docs.iterator()));
    when(mockAttributeMapping.getIdentifierAttributeId(TEST_ENTITY_TYPE))
        .thenReturn(Optional.of("API.id"));

    DeleteEntitiesRequest request =
        DeleteEntitiesRequest.newBuilder()
            .setEntityType(TEST_ENTITY_TYPE)
            .setFilter(
                org.hypertrace.entity.query.service.v1.Filter.newBuilder()
                    .setLhs(
                        Expression.newBuilder()
                            .setColumnIdentifier(
                                ColumnIdentifier.newBuilder().setColumnName(ATTRIBUTE_ID1).build())
                            .build())
                    .setOperator(Operator.EQ)
                    .setRhs(
                        Expression.newBuilder()
                            .setLiteral(
                                LiteralConstant.newBuilder()
                                    .setValue(
                                        Value.newBuilder().setString(entityId.toString()).build())
                                    .build())
                            .build())
                    .build())
            .build();
    StreamObserver<DeleteEntitiesResponse> mockResponseObserver = mock(StreamObserver.class);

    Context.current()
        .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
        .run(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(
                      mockEntitiesCollection,
                      mockMappingForAttributes(),
                      entityChangeEventGenerator,
                      entityFetcher,
                      100,
                      false,
                      1000);

              eqs.deleteEntities(request, mockResponseObserver);
            });

    DeleteEntitiesResponse response =
        DeleteEntitiesResponse.newBuilder().addEntityIds(entityId.toString()).build();
    verify(mockEntitiesCollection, times(1)).delete(any(Filter.class));
    verify(mockResponseObserver, times(1)).onNext(eq(response));
    verify(mockResponseObserver, times(1)).onCompleted();
  }

  @Test
  @Disabled("Disabled until we enable querying based on the new Query DTO")
  public void testExecute_withAliases() throws Exception {
    Collection mockEntitiesCollection = mock(Collection.class);
    List<Document> docs =
        List.of(
            new JSONDocument(
                "{\n"
                    + "    \"tenantId\": \"tenant-1\",\n"
                    + "    \"entityId\": \""
                    + UUID.randomUUID()
                    + "\",\n"
                    + "    \"entityType\": \""
                    + TEST_ENTITY_TYPE
                    + "\",\n"
                    + "    \"entityName\": \"Test entity 1\",\n"
                    + "    \"col1\": \"col1-value\",\n"
                    + "    \"Entity\": \n"
                    + "    {\n"
                    + "        \"status\":\n"
                    + "        {\n"
                    + "            \"value\":\n"
                    + "            {\n"
                    + "                \"string\": \"col2-value\"\n"
                    + "            }\n"
                    + "        }\n"
                    + "    }\n"
                    + "}"));
    when(mockEntitiesCollection.aggregate(any()))
        .thenReturn(convertToCloseableIterator(docs.iterator()));
    when(mockEntitiesCollection.search(any()))
        .thenReturn(convertToCloseableIterator(docs.iterator()));

    EntityQueryRequest request =
        EntityQueryRequest.newBuilder()
            .setEntityType(TEST_ENTITY_TYPE)
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder()
                            .setColumnName(ATTRIBUTE_ID1)
                            .setAlias("col1")))
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName(ATTRIBUTE_ID2)))
            .build();
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);

    Context.current()
        .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
        .run(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(
                      mockEntitiesCollection,
                      mockMappingForAttributes1And2(),
                      entityChangeEventGenerator,
                      entityFetcher,
                      100,
                      false,
                      1000);

              eqs.execute(request, mockResponseObserver);
            });

    ResultSetChunk expectedResponse =
        ResultSetChunk.newBuilder()
            .setIsLastChunk(true)
            .setResultSetMetadata(
                ResultSetMetadata.newBuilder()
                    .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("col1"))
                    .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName(ATTRIBUTE_ID2)))
            .addRow(
                Row.newBuilder()
                    .addColumn(Value.newBuilder().setString("col1-value"))
                    .addColumn(Value.newBuilder().setString("col2-value"))
                    .build())
            .build();
    verify(mockResponseObserver, times(1)).onNext(eq(expectedResponse));
    verify(mockResponseObserver, times(1)).onCompleted();
  }

  @Nested
  class TotalEntities {

    @DisplayName("should build correct doc store query for total")
    @Test
    public void test_buildTotalQuery() throws Exception {
      TotalEntitiesRequest totalEntitiesRequest =
          TotalEntitiesRequest.newBuilder()
              .setEntityType(TEST_ENTITY_TYPE)
              .setFilter(org.hypertrace.entity.query.service.v1.Filter.getDefaultInstance())
              .build();

      ArgumentCaptor<Query> docStoreQueryCaptor = ArgumentCaptor.forClass(Query.class);

      EntityQueryServiceImpl eqs =
          new EntityQueryServiceImpl(
              entitiesCollection,
              mockAttributeMapping,
              entityChangeEventGenerator,
              entityFetcher,
              1,
              false,
              1000);
      StreamObserver<TotalEntitiesResponse> mockResponseObserver = mock(StreamObserver.class);

      Context.current()
          .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
          .call(
              () -> {
                eqs.total(totalEntitiesRequest, mockResponseObserver);
                return null;
              });

      verify(entitiesCollection, times(1)).total(docStoreQueryCaptor.capture());
      Query query = docStoreQueryCaptor.getValue();
      assertEquals(Filter.Op.AND, query.getFilter().getOp());
      assertEquals(2, query.getFilter().getChildFilters().length);
      // tenant id filter
      assertEquals(Filter.Op.EQ, query.getFilter().getChildFilters()[0].getOp());
      assertEquals("tenantId", query.getFilter().getChildFilters()[0].getFieldName());
      assertEquals("tenant1", query.getFilter().getChildFilters()[0].getValue());

      // entity type filter
      assertEquals(Filter.Op.EQ, query.getFilter().getChildFilters()[1].getOp());
      assertEquals("entityType", query.getFilter().getChildFilters()[1].getFieldName());
      assertEquals(TEST_ENTITY_TYPE, query.getFilter().getChildFilters()[1].getValue());
    }

    @DisplayName("should send correct total response")
    @Test
    public void test_sendCorrectTotalResponse() throws Exception {
      TotalEntitiesRequest totalEntitiesRequest =
          TotalEntitiesRequest.newBuilder()
              .setEntityType(TEST_ENTITY_TYPE)
              .setFilter(org.hypertrace.entity.query.service.v1.Filter.getDefaultInstance())
              .build();

      EntityQueryServiceImpl eqs =
          new EntityQueryServiceImpl(
              entitiesCollection,
              mockAttributeMapping,
              entityChangeEventGenerator,
              entityFetcher,
              1,
              false,
              1000);
      StreamObserver<TotalEntitiesResponse> mockResponseObserver = mock(StreamObserver.class);

      when(entitiesCollection.total(any())).thenReturn(123L);
      Context.current()
          .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
          .call(
              () -> {
                eqs.total(totalEntitiesRequest, mockResponseObserver);
                return null;
              });

      verify(mockResponseObserver, times(1))
          .onNext(TotalEntitiesResponse.newBuilder().setTotal(123L).build());
      verify(mockResponseObserver, times(1)).onCompleted();
    }
  }

  private Collection mockEntitiesCollection() throws Exception {
    // mock successful update
    try {
      return when(entitiesCollection.bulkUpdateSubDocs(any()))
          .thenReturn(new BulkUpdateResult(0))
          .getMock();
    } catch (Exception e) {
      throw e;
    }
  }

  private RequestContext mockRequestContextWithTenantId() {
    // mock successful update
    return when(requestContext.getTenantId()).thenReturn(Optional.of("tenant1")).getMock();
  }

  private EntityAttributeMapping mockMappingForAttributes() {
    return when(this.mockMappingForAttribute1()
            .getDocStorePathByAttributeId(requestContext, API_ID))
        .thenReturn(Optional.of(EDS_API_ID_COLUMN_NAME))
        .getMock();
  }

  private EntityAttributeMapping mockMappingForAttribute1() {
    return when(mockAttributeMapping.getDocStorePathByAttributeId(requestContext, ATTRIBUTE_ID1))
        .thenReturn(Optional.of(EDS_COLUMN_NAME1))
        .getMock();
  }

  private EntityAttributeMapping mockMappingForAttributes1And2() {
    return when(this.mockMappingForAttribute1()
            .getDocStorePathByAttributeId(requestContext, ATTRIBUTE_ID2))
        .thenReturn(Optional.of(EDS_COLUMN_NAME2))
        .getMock();
  }

  static class ExceptionMessageMatcher implements ArgumentMatcher<Exception> {

    private final String expectedMessage;

    ExceptionMessageMatcher(String expectedMessage) {
      this.expectedMessage = expectedMessage;
    }

    @Override
    public boolean matches(Exception argument) {
      return argument.getMessage().equals(expectedMessage);
    }
  }
}
