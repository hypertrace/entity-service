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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
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
import org.hypertrace.entity.query.service.v1.UpdateOperation;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.service.constants.EntityServiceConstants;
import org.hypertrace.entity.service.util.DocStoreJsonFormat;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

public class EntityQueryServiceImplTest {

  private final Map<String, Map<String, String>> attributeFqnMaps = new HashMap<>();
  private static final String TEST_ENTITY_TYPE = "TEST_ENTITY";

  private static final String EQS_COLUMN_NAME1 = "Entity.id";
  private static final String EDS_COLUMN_NAME1 = "attributes.entity_id";
  private static final String EQS_COLUMN_NAME2 = "Entity.status";
  private static final String EDS_COLUMN_NAME2 = "attributes.status";

  public EntityQueryServiceImplTest() {
    // Init attribute FQN mapping needed for tests here
    Map<String, String> entityAttributeFqnMap = new HashMap<>();
    entityAttributeFqnMap.put(EQS_COLUMN_NAME1, EDS_COLUMN_NAME1);
    entityAttributeFqnMap.put(EQS_COLUMN_NAME2, EDS_COLUMN_NAME2);

    attributeFqnMaps.put(TEST_ENTITY_TYPE, entityAttributeFqnMap);
  }

  @Test
  public void testUpdate_noTenantId() throws Exception {
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);

    Context.current()
        .withValue(RequestContext.CURRENT, mock(RequestContext.class))
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(mockEntitiesCollection(), attributeFqnMaps, 1);

              eqs.update(null, mockResponseObserver);

              verify(mockResponseObserver, times(1))
                  .onError(argThat(
                      new ExceptionMessageMatcher("Tenant id is missing in the request.")));
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
                  new EntityQueryServiceImpl(mockEntitiesCollection(), attributeFqnMaps, 1);

              eqs.update(EntityUpdateRequest.newBuilder().build(), mockResponseObserver);

              verify(mockResponseObserver, times(1))
                  .onError(argThat(
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
                  new EntityQueryServiceImpl(mockEntitiesCollection(), attributeFqnMaps, 1);

              eqs.update(EntityUpdateRequest.newBuilder()
                  .setEntityType(TEST_ENTITY_TYPE)
                  .build(), mockResponseObserver);

              verify(mockResponseObserver, times(1))
                  .onError(argThat(
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
                  new EntityQueryServiceImpl(mockEntitiesCollection(), attributeFqnMaps, 1);

              eqs.update(EntityUpdateRequest.newBuilder()
                  .setEntityType(TEST_ENTITY_TYPE)
                  .addEntityIds("entity-id-1")
                  .build(), mockResponseObserver);

              verify(mockResponseObserver, times(1))
                  .onError(argThat(
                      new ExceptionMessageMatcher("Operation is missing in the request.")));
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
                                ColumnIdentifier.newBuilder().setColumnName("Entity.status"))
                            .setValue(newStatus)))
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("Entity.id")))
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("Entity.status")))
            .build();

    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);

    Context.current()
        .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(mockEntitiesCollection, attributeFqnMaps, 1);
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
        .withValue(RequestContext.CURRENT, mock(RequestContext.class))
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(mockEntitiesCollection(), attributeFqnMaps, 1);

              eqs.execute(null, mockResponseObserver);

              verify(mockResponseObserver, times(1))
                  .onError(argThat(
                      new ExceptionMessageMatcher("Tenant id is missing in the request.")));
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
                AttributeValue.newBuilder().setValue(
                    org.hypertrace.entity.data.service.v1.Value.newBuilder().setString("foo1")).build())
            .build();
    Entity entity2 =
        Entity.newBuilder()
            .setTenantId("tenant-1")
            .setEntityType(TEST_ENTITY_TYPE)
            .setEntityId(UUID.randomUUID().toString())
            .setEntityName("Test entity 2")
            .putAttributes(
                EDS_COLUMN_NAME1,
                AttributeValue.newBuilder().setValue(
                    org.hypertrace.entity.data.service.v1.Value.newBuilder().setString("foo2")).build())
            .build();

    List<Document> docs = List.of(
        new JSONDocument(JsonFormat.printer().print(entity1)),
        new JSONDocument(JsonFormat.printer().print(entity2)),
        // this doc will result in parsing error
        new JSONDocument("{\"entityId\": [1, 2]}"));
    when(mockEntitiesCollection.search(any())).thenReturn(docs.iterator());
    EntityQueryRequest request = EntityQueryRequest.newBuilder()
        .setEntityType(TEST_ENTITY_TYPE)
        .addOrderBy(
            OrderByExpression.newBuilder()
                .setExpression(
                    Expression.newBuilder().setColumnIdentifier(
                        ColumnIdentifier.newBuilder()
                            .setColumnName(EQS_COLUMN_NAME1)
                            .build()))
        ).build();
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);
    Context.current()
        .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(mockEntitiesCollection, attributeFqnMaps, 1);

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
                AttributeValue.newBuilder().setValue(
                    org.hypertrace.entity.data.service.v1.Value.newBuilder().setString("foo1")).build())
            .build();
    Entity entity2 =
        Entity.newBuilder()
            .setTenantId("tenant-1")
            .setEntityType(TEST_ENTITY_TYPE)
            .setEntityId(UUID.randomUUID().toString())
            .setEntityName("Test entity 2")
            .putAttributes(
                EDS_COLUMN_NAME1,
                AttributeValue.newBuilder().setValue(
                    org.hypertrace.entity.data.service.v1.Value.newBuilder().setString("foo2")).build())
            .build();

    Entity entity3 =
        Entity.newBuilder()
            .setTenantId("tenant-1")
            .setEntityType(TEST_ENTITY_TYPE)
            .setEntityId(UUID.randomUUID().toString())
            .setEntityName("Test entity 3")
            .putAttributes(
                EDS_COLUMN_NAME1,
                AttributeValue.newBuilder().setValue(
                    org.hypertrace.entity.data.service.v1.Value.newBuilder().setString("foo2")).build())
            .build();

    List<Document> docs = List.of(
        new JSONDocument(JsonFormat.printer().print(entity1)),
        new JSONDocument(JsonFormat.printer().print(entity2)),
        new JSONDocument(JsonFormat.printer().print(entity3)),
        // this doc will result in parsing error
        new JSONDocument("{\"entityId\": [1, 2]}"));
    when(mockEntitiesCollection.search(any())).thenReturn(docs.iterator());
    EntityQueryRequest request = EntityQueryRequest.newBuilder()
        .setEntityType(TEST_ENTITY_TYPE)
        .addOrderBy(
            OrderByExpression.newBuilder()
                .setExpression(
                    Expression.newBuilder().setColumnIdentifier(
                        ColumnIdentifier.newBuilder()
                            .setColumnName(EQS_COLUMN_NAME1)
                            .build()))
        ).build();
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);
    Context.current()
        .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(mockEntitiesCollection, attributeFqnMaps, 2);

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
                AttributeValue.newBuilder().setValue(
                    org.hypertrace.entity.data.service.v1.Value.newBuilder().setString("doing good")).build())
            .build();

    List<Expression> selections = Lists.newArrayList();
    selections.add(Expression.newBuilder().setColumnIdentifier(
        ColumnIdentifier.newBuilder().setColumnName("entity_id").build()).build());
    selections.add(Expression.newBuilder().setColumnIdentifier(
        ColumnIdentifier.newBuilder().setColumnName("entity_name").build()).build());
    selections.add(Expression.newBuilder().setColumnIdentifier(
        ColumnIdentifier.newBuilder().setColumnName("query_status").build()).build());

    Row row = EntityQueryServiceImpl.convertToEntityQueryResult(
        entity, selections,
        Map.of(
            "entity_id", EntityServiceConstants.ENTITY_ID,
        "entity_name", EntityServiceConstants.ENTITY_NAME,
        "query_status", "attributes.status"));

    assertEquals(entityId, row.getColumn(0).getString());
    assertEquals(entityName, row.getColumn(1).getString());
    assertEquals("doing good", row.getColumn(2).getString());
  }

  private RequestContext mockRequestContextWithTenantId() {
    RequestContext mockRequestContext = mock(RequestContext.class);
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of("tenant1"));
    return mockRequestContext;
  }

  private Collection mockEntitiesCollection() {
    Collection mockEntitiesCollection = mock(Collection.class);
    // mock successful update
    when(mockEntitiesCollection.updateSubDoc(any(), any(), any())).thenReturn(true);
    return mockEntitiesCollection;
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
