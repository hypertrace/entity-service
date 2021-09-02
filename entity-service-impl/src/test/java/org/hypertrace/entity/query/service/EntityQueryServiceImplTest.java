package org.hypertrace.entity.query.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.EntityUpdateRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.LiteralConstant.Builder;
import org.hypertrace.entity.query.service.v1.OrderByExpression;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.SetAttribute;
import org.hypertrace.entity.query.service.v1.TotalEntitiesRequest;
import org.hypertrace.entity.query.service.v1.TotalEntitiesResponse;
import org.hypertrace.entity.query.service.v1.UpdateOperation;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.service.constants.EntityServiceConstants;
import org.hypertrace.entity.service.util.DocStoreJsonFormat;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EntityQueryServiceImplTest {

  private static final String TEST_ENTITY_TYPE = "TEST_ENTITY";
  @Mock RequestContext requestContext;
  @Mock EntityAttributeMapping mockAttributeMapping;
  @Mock Collection entitiesCollection;

  private static final String ATTRIBUTE_ID1 = "Entity.id";
  private static final String EDS_COLUMN_NAME1 = "attributes.entity_id";
  private static final String ATTRIBUTE_ID2 = "Entity.status";
  private static final String EDS_COLUMN_NAME2 = "attributes.status";

  @Test
  public void testUpdate_noTenantId() throws Exception {
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);
    when(requestContext.getTenantId()).thenReturn(Optional.empty());
    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(entitiesCollection, mockAttributeMapping, 1);

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
                  new EntityQueryServiceImpl(entitiesCollection, mockAttributeMapping, 1);

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
                  new EntityQueryServiceImpl(entitiesCollection, mockAttributeMapping, 1);

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
                  new EntityQueryServiceImpl(entitiesCollection, mockAttributeMapping, 1);

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
                      mockEntitiesCollection, mockMappingForAttributes1And2(), 1);
              eqs.update(updateRequest, mockResponseObserver);
              return null;
            });

    verify(mockEntitiesCollection, times(1))
        .updateSubDoc(
            eq(new SingleValueKey("tenant1", "entity-id-1")),
            eq("attributes.status"),
            eq(new JSONDocument(DocStoreJsonFormat.printer().print(newStatus))));
  }

  @Test
  public void testExecute_noTenantId() throws Exception {
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);
    Context.current()
        .withValue(RequestContext.CURRENT, requestContext)
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(entitiesCollection, mockAttributeMapping, 1);

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
    when(mockEntitiesCollection.search(any())).thenReturn(docs.iterator());
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
                  new EntityQueryServiceImpl(mockEntitiesCollection, mockMappingForAttribute1(), 1);

              eqs.execute(request, mockResponseObserver);
              return null;
            });

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
    when(mockEntitiesCollection.search(any())).thenReturn(docs.iterator());
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
                  new EntityQueryServiceImpl(mockEntitiesCollection, mockMappingForAttribute1(), 2);

              eqs.execute(request, mockResponseObserver);
              return null;
            });

    verify(mockEntitiesCollection, times(1)).search(any());
    verify(mockResponseObserver, times(2)).onNext(any());
    verify(mockResponseObserver, times(1)).onCompleted();
  }

  @Test
  public void testConvertToEntityQueryResult() {
    String entityId = UUID.randomUUID().toString();
    String entityName = UUID.randomUUID().toString();
    Entity entity =
        Entity.newBuilder()
            .setTenantId("tenant-1")
            .setEntityType(TEST_ENTITY_TYPE)
            .setEntityId(entityId)
            .setEntityName(entityName)
            .putAttributes(
                "status",
                AttributeValue.newBuilder()
                    .setValue(
                        org.hypertrace.entity.data.service.v1.Value.newBuilder()
                            .setString("doing good"))
                    .build())
            .build();

    List<Expression> selections = Lists.newArrayList();
    selections.add(
        Expression.newBuilder()
            .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("entity_id"))
            .build());
    selections.add(
        Expression.newBuilder()
            .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("entity_name"))
            .build());
    selections.add(
        Expression.newBuilder()
            .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("query_status"))
            .build());

    when(mockAttributeMapping.getDocStorePathByAttributeId(requestContext, "entity_id"))
        .thenReturn(Optional.of(EntityServiceConstants.ENTITY_ID));
    when(mockAttributeMapping.getDocStorePathByAttributeId(requestContext, "entity_name"))
        .thenReturn(Optional.of(EntityServiceConstants.ENTITY_NAME));
    when(mockAttributeMapping.getDocStorePathByAttributeId(requestContext, "query_status"))
        .thenReturn(Optional.of("attributes.status"));

    Row row =
        new EntityQueryServiceImpl(entitiesCollection, mockAttributeMapping, 1)
            .convertToEntityQueryResult(requestContext, entity, selections);

    assertEquals(entityId, row.getColumn(0).getString());
    assertEquals(entityName, row.getColumn(1).getString());
    assertEquals("doing good", row.getColumn(2).getString());
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
          new EntityQueryServiceImpl(entitiesCollection, mockAttributeMapping, 1);
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
          new EntityQueryServiceImpl(entitiesCollection, mockAttributeMapping, 1);
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

  private Collection mockEntitiesCollection() {
    // mock successful update
    return when(entitiesCollection.updateSubDoc(any(), any(), any())).thenReturn(true).getMock();
  }

  private RequestContext mockRequestContextWithTenantId() {
    // mock successful update
    return when(requestContext.getTenantId()).thenReturn(Optional.of("tenant1")).getMock();
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
