package org.hypertrace.entity.query.service;

import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_STRING;
import static org.hypertrace.core.documentstore.expression.impl.LogicalExpression.and;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.NONE;
import static org.hypertrace.entity.TestUtils.convertToCloseableIterator;
import static org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator.ATTRIBUTE_UPDATE_OPERATOR_SET;
import static org.hypertrace.entity.service.constants.EntityConstants.ENTITY_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import com.google.protobuf.util.JsonFormat;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Collections;
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
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeChangeEvaluator;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.fetcher.EntityFetcher;
import org.hypertrace.entity.metric.EntityCounterMetricSender;
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
import org.hypertrace.entity.query.service.v1.Update;
import org.hypertrace.entity.query.service.v1.UpdateOperation;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.service.change.event.api.EntityChangeEventGenerator;
import org.hypertrace.entity.service.util.DocStoreJsonFormat;
import org.hypertrace.entity.v1.entitytype.EntityType;
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
  private final String TENANT_ID = "tenant1";
  @Mock RequestContext requestContext;
  @Mock EntityAttributeMapping mockAttributeMapping;
  @Mock Collection entitiesCollection;
  @Mock EntityChangeEventGenerator entityChangeEventGenerator;
  @Mock EntityFetcher entityFetcher;
  @Mock EntityAttributeChangeEvaluator entityAttributeChangeEvaluator;

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
                      mock(Datastore.class),
                      mockAttributeMapping,
                      entityChangeEventGenerator,
                      entityAttributeChangeEvaluator,
                      new EntityCounterMetricSender(),
                      entityFetcher,
                      mock(Channel.class),
                      1,
                      1000,
                      5000);

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
                      mock(Datastore.class),
                      mockAttributeMapping,
                      entityChangeEventGenerator,
                      entityAttributeChangeEvaluator,
                      new EntityCounterMetricSender(),
                      entityFetcher,
                      mock(Channel.class),
                      1,
                      1000,
                      5000);

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
                      mock(Datastore.class),
                      mockAttributeMapping,
                      entityChangeEventGenerator,
                      entityAttributeChangeEvaluator,
                      new EntityCounterMetricSender(),
                      entityFetcher,
                      mock(Channel.class),
                      1,
                      1000,
                      5000);

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
                      mock(Datastore.class),
                      mockAttributeMapping,
                      entityChangeEventGenerator,
                      entityAttributeChangeEvaluator,
                      new EntityCounterMetricSender(),
                      entityFetcher,
                      mock(Channel.class),
                      1,
                      1000,
                      5000);

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
                      mock(Datastore.class),
                      mockMappingForAttributes1And2(),
                      entityChangeEventGenerator,
                      entityAttributeChangeEvaluator,
                      new EntityCounterMetricSender(),
                      entityFetcher,
                      mock(Channel.class),
                      1,
                      1000,
                      5000);
              eqs.update(updateRequest, mockResponseObserver);
              return null;
            });

    verify(mockEntitiesCollection, times(1))
        .bulkUpdateSubDocs(
            eq(
                Map.of(
                    new SingleValueKey(TENANT_ID, "entity-id-1"),
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
                        mock(Datastore.class),
                        mockAttributeMapping,
                        entityChangeEventGenerator,
                        entityAttributeChangeEvaluator,
                        new EntityCounterMetricSender(),
                        entityFetcher,
                        mock(Channel.class),
                        1,
                        1000,
                        5000);

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
                        mock(Datastore.class),
                        mockAttributeMapping,
                        entityChangeEventGenerator,
                        entityAttributeChangeEvaluator,
                        new EntityCounterMetricSender(),
                        entityFetcher,
                        mock(Channel.class),
                        1,
                        1000,
                        5000);

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
                        mock(Datastore.class),
                        mockAttributeMapping,
                        entityChangeEventGenerator,
                        entityAttributeChangeEvaluator,
                        new EntityCounterMetricSender(),
                        entityFetcher,
                        mock(Channel.class),
                        1,
                        1000,
                        5000);

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
                        mock(Datastore.class),
                        mockAttributeMapping,
                        entityChangeEventGenerator,
                        entityAttributeChangeEvaluator,
                        new EntityCounterMetricSender(),
                        entityFetcher,
                        mock(Channel.class),
                        1,
                        1000,
                        5000);
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
                        mock(Datastore.class),
                        mockMappingForAttribute1(),
                        entityChangeEventGenerator,
                        entityAttributeChangeEvaluator,
                        new EntityCounterMetricSender(),
                        entityFetcher,
                        mock(Channel.class),
                        1,
                        1000,
                        5000);
                eqs.bulkUpdate(bulkUpdateRequest, mockResponseObserver);
                return null;
              });

      verify(mockEntitiesCollection, times(1))
          .bulkUpdateSubDocs(
              eq(
                  Map.of(
                      new SingleValueKey(TENANT_ID, "entity-id-1"),
                      Map.of(
                          "attributes.entity_id",
                          new JSONDocument(DocStoreJsonFormat.printer().print(newStatus))))));
    }
  }

  @SuppressWarnings("unchecked")
  @Nested
  class BulkUpdateAllMatchingFilter {

    @Test
    void testBulkUpdateAllMatchingFilter_noTenantId() throws Exception {
      final StreamObserver<BulkUpdateAllMatchingFilterResponse> mockResponseObserver =
          mock(StreamObserver.class);
      when(requestContext.getTenantId()).thenReturn(Optional.empty());
      Context.current()
          .withValue(RequestContext.CURRENT, requestContext)
          .call(
              () -> {
                final EntityQueryServiceImpl eqs =
                    new EntityQueryServiceImpl(
                        entitiesCollection,
                        mock(Datastore.class),
                        mockAttributeMapping,
                        entityChangeEventGenerator,
                        entityAttributeChangeEvaluator,
                        new EntityCounterMetricSender(),
                        entityFetcher,
                        mock(Channel.class),
                        1,
                        1000,
                        5000);

                eqs.bulkUpdateAllMatchingFilter(null, mockResponseObserver);

                verify(mockResponseObserver, times(1))
                    .onError(
                        argThat(
                            new ExceptionMessagePartialMatcher(
                                "Tenant id is missing in the request.")));
                return null;
              });
    }

    @Test
    void testBulkUpdateAllMatchingFilter_noEntityType() throws Exception {
      final StreamObserver<BulkUpdateAllMatchingFilterResponse> mockResponseObserver =
          mock(StreamObserver.class);

      Context.current()
          .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
          .call(
              () -> {
                final EntityQueryServiceImpl eqs =
                    new EntityQueryServiceImpl(
                        entitiesCollection,
                        mock(Datastore.class),
                        mockAttributeMapping,
                        entityChangeEventGenerator,
                        entityAttributeChangeEvaluator,
                        new EntityCounterMetricSender(),
                        entityFetcher,
                        mock(Channel.class),
                        1,
                        1000,
                        5000);

                eqs.bulkUpdateAllMatchingFilter(
                    BulkUpdateAllMatchingFilterRequest.newBuilder().build(), mockResponseObserver);

                verify(mockResponseObserver, times(1))
                    .onError(
                        argThat(
                            new ExceptionMessagePartialMatcher(
                                "Entity type is missing in the request.")));
                return null;
              });
    }

    @Test
    void testBulkUpdateAllMatchingFilter_entitiesWithNoUpdateOperations() throws Exception {
      final BulkUpdateAllMatchingFilterRequest bulkUpdateRequest =
          BulkUpdateAllMatchingFilterRequest.newBuilder().setEntityType(TEST_ENTITY_TYPE).build();

      final StreamObserver<BulkUpdateAllMatchingFilterResponse> mockResponseObserver =
          mock(StreamObserver.class);

      Context.current()
          .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
          .call(
              () -> {
                final EntityQueryServiceImpl eqs =
                    new EntityQueryServiceImpl(
                        entitiesCollection,
                        mock(Datastore.class),
                        mockAttributeMapping,
                        entityChangeEventGenerator,
                        entityAttributeChangeEvaluator,
                        new EntityCounterMetricSender(),
                        entityFetcher,
                        mock(Channel.class),
                        1,
                        1000,
                        5000);
                eqs.bulkUpdateAllMatchingFilter(bulkUpdateRequest, mockResponseObserver);

                verify(mockResponseObserver, times(1))
                    .onError(
                        argThat(
                            new ExceptionMessagePartialMatcher(
                                "No operation is specified in the request.")));
                return null;
              });
      verify(entitiesCollection, Mockito.never()).bulkUpdate(any(), anyCollection(), any());
    }

    @Test
    void testBulkUpdateAllMatchingFilter_success() throws Exception {
      final Collection mockEntitiesCollection = mockEntitiesCollection();

      final Builder newStatus =
          LiteralConstant.newBuilder()
              .setValue(Value.newBuilder().setValueType(ValueType.STRING).setString("NEW_STATUS"));

      final AttributeUpdateOperation updateOperation =
          AttributeUpdateOperation.newBuilder()
              .setAttribute(ColumnIdentifier.newBuilder().setColumnName(ATTRIBUTE_ID1))
              .setOperator(ATTRIBUTE_UPDATE_OPERATOR_SET)
              .setValue(newStatus)
              .build();
      final String entityId = "entity-id-1";
      final BulkUpdateAllMatchingFilterRequest bulkUpdateRequest =
          BulkUpdateAllMatchingFilterRequest.newBuilder()
              .setEntityType(EntityType.API.name())
              .addUpdates(
                  Update.newBuilder()
                      .setFilter(
                          org.hypertrace.entity.query.service.v1.Filter.newBuilder()
                              .setLhs(
                                  Expression.newBuilder()
                                      .setColumnIdentifier(
                                          ColumnIdentifier.newBuilder()
                                              .setColumnName(ATTRIBUTE_ID1)))
                              .setOperator(Operator.EQ)
                              .setRhs(
                                  Expression.newBuilder()
                                      .setLiteral(
                                          LiteralConstant.newBuilder()
                                              .setValue(
                                                  Value.newBuilder()
                                                      .setValueType(ValueType.STRING)
                                                      .setString(entityId)))))
                      .addOperations(updateOperation))
              .build();

      final StreamObserver<BulkUpdateAllMatchingFilterResponse> mockResponseObserver =
          mock(StreamObserver.class);

      final List<Entity> existingEntities =
          Collections.singletonList(
              Entity.newBuilder()
                  .setTenantId("tenant1")
                  .setEntityId(entityId)
                  .setEntityType("API")
                  .build());
      when(mockMappingForAttributes().getIdentifierAttributeId(EntityType.API.name()))
          .thenReturn(Optional.of(ENTITY_ID));

      final org.hypertrace.core.documentstore.query.Query query =
          org.hypertrace.core.documentstore.query.Query.builder()
              .setFilter(
                  and(
                      RelationalExpression.of(
                          IdentifierExpression.of("attributes.entity_id.value.string"),
                          RelationalOperator.EQ,
                          ConstantExpression.of(entityId)),
                      RelationalExpression.of(
                          IdentifierExpression.of("tenantId"),
                          RelationalOperator.EQ,
                          ConstantExpression.of(TENANT_ID)),
                      RelationalExpression.of(
                          IdentifierExpression.of("entityType"),
                          RelationalOperator.EQ,
                          ConstantExpression.of(EntityType.API.name()))))
              .build();
      when(entityFetcher.query(query)).thenReturn(existingEntities);

      Context.current()
          .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
          .call(
              () -> {
                final EntityQueryServiceImpl eqs =
                    new EntityQueryServiceImpl(
                        mockEntitiesCollection,
                        mock(Datastore.class),
                        mockMappingForAttribute1(),
                        entityChangeEventGenerator,
                        entityAttributeChangeEvaluator,
                        new EntityCounterMetricSender(),
                        entityFetcher,
                        mock(Channel.class),
                        1,
                        1000,
                        5000);
                eqs.bulkUpdateAllMatchingFilter(bulkUpdateRequest, mockResponseObserver);
                return null;
              });

      final ArgumentCaptor<List<SubDocumentUpdate>> valueCaptor =
          ArgumentCaptor.forClass(List.class);

      verify(mockEntitiesCollection, times(1))
          .bulkUpdate(
              eq(query),
              eq(
                  List.of(
                      SubDocumentUpdate.of(
                          "attributes.entity_id",
                          SubDocumentValue.of(
                              new JSONDocument("{\"value\":{\"string\":\"NEW_STATUS\"}}"))))),
              eq(UpdateOptions.builder().returnDocumentType(NONE).build()));
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
                      mock(Datastore.class),
                      mockAttributeMapping,
                      entityChangeEventGenerator,
                      entityAttributeChangeEvaluator,
                      new EntityCounterMetricSender(),
                      entityFetcher,
                      mock(Channel.class),
                      1,
                      1000,
                      5000);

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
                      mock(Datastore.class),
                      mockMappingForAttributes1And2(),
                      entityChangeEventGenerator,
                      entityAttributeChangeEvaluator,
                      new EntityCounterMetricSender(),
                      mock(Channel.class),
                      1,
                      1000,
                      5000);

              eqs.execute(request, mockResponseObserver);
              return null;
            });

    verify(mockEntitiesCollection, times(1)).aggregate(any());
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
                      mock(Datastore.class),
                      mockMappingForAttributes1And2(),
                      entityChangeEventGenerator,
                      entityAttributeChangeEvaluator,
                      new EntityCounterMetricSender(),
                      mock(Channel.class),
                      2,
                      1000,
                      5000);

              eqs.execute(request, mockResponseObserver);
              return null;
            });

    verify(mockEntitiesCollection, times(1)).aggregate(any());
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
                      mock(Datastore.class),
                      mockAttributeMapping,
                      entityChangeEventGenerator,
                      entityAttributeChangeEvaluator,
                      new EntityCounterMetricSender(),
                      entityFetcher,
                      mock(Channel.class),
                      1,
                      1000,
                      5000);
              eqs.bulkUpdateEntityArrayAttribute(request, mockResponseObserver);
              return null;
            });

    ArgumentCaptor<BulkArrayValueUpdateRequest> argumentCaptor =
        ArgumentCaptor.forClass(BulkArrayValueUpdateRequest.class);
    verify(entitiesCollection, times(1)).bulkOperationOnArrayValue(argumentCaptor.capture());
    BulkArrayValueUpdateRequest bulkArrayValueUpdateRequest = argumentCaptor.getValue();
    assertEquals(
        entityIds.stream()
            .map(entityId -> new SingleValueKey(TENANT_ID, entityId))
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
  public void testDeleteEntities() throws IOException {
    Collection mockEntitiesCollection = mock(Collection.class);
    UUID entityId = UUID.randomUUID();
    List<Entity> docs = List.of(Entity.newBuilder().setEntityId(entityId.toString()).build());

    when(this.entityFetcher.query(any(org.hypertrace.core.documentstore.query.Query.class)))
        .thenReturn(docs);
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
                      mock(Datastore.class),
                      mockMappingForAttributes(),
                      entityChangeEventGenerator,
                      entityAttributeChangeEvaluator,
                      new EntityCounterMetricSender(),
                      entityFetcher,
                      mock(Channel.class),
                      100,
                      1000,
                      5000);

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
                      mock(Datastore.class),
                      mockMappingForAttributes1And2(),
                      entityChangeEventGenerator,
                      entityAttributeChangeEvaluator,
                      new EntityCounterMetricSender(),
                      entityFetcher,
                      mock(Channel.class),
                      100,
                      1000,
                      5000);

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

      EntityQueryServiceImpl eqs =
          new EntityQueryServiceImpl(
              entitiesCollection,
              mock(Datastore.class),
              mockAttributeMapping,
              entityChangeEventGenerator,
              entityAttributeChangeEvaluator,
              new EntityCounterMetricSender(),
              entityFetcher,
              mock(Channel.class),
              1,
              1000,
              5000);
      StreamObserver<TotalEntitiesResponse> mockResponseObserver = mock(StreamObserver.class);

      Context.current()
          .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
          .call(
              () -> {
                eqs.total(totalEntitiesRequest, mockResponseObserver);
                return null;
              });

      verify(entitiesCollection, times(1))
          .count(
              Query.builder()
                  .setFilter(
                      org.hypertrace.core.documentstore.query.Filter.builder()
                          .expression(
                              LogicalExpression.and(
                                  List.of(
                                      RelationalExpression.of(
                                          IdentifierExpression.of("tenantId"),
                                          RelationalOperator.EQ,
                                          ConstantExpression.of(TENANT_ID)),
                                      RelationalExpression.of(
                                          IdentifierExpression.of("entityType"),
                                          RelationalOperator.EQ,
                                          ConstantExpression.of(TEST_ENTITY_TYPE)))))
                          .build())
                  .build());
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
              mock(Datastore.class),
              mockAttributeMapping,
              entityChangeEventGenerator,
              entityAttributeChangeEvaluator,
              new EntityCounterMetricSender(),
              entityFetcher,
              mock(Channel.class),
              1,
              1000,
              5000);
      StreamObserver<TotalEntitiesResponse> mockResponseObserver = mock(StreamObserver.class);

      when(entitiesCollection.count(any())).thenReturn(123L);
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
    return when(requestContext.getTenantId()).thenReturn(Optional.of(TENANT_ID)).getMock();
  }

  private EntityAttributeMapping mockMappingForAttributes() {
    when(mockAttributeMapping.getAttributeKind(requestContext, API_ID))
        .thenReturn(Optional.of(TYPE_STRING));
    when(mockAttributeMapping.isPrimitive(requestContext, ATTRIBUTE_ID1)).thenReturn(true);
    return when(this.mockMappingForAttribute1()
            .getDocStorePathByAttributeId(requestContext, API_ID))
        .thenReturn(Optional.of(EDS_API_ID_COLUMN_NAME))
        .getMock();
  }

  private EntityAttributeMapping mockMappingForAttribute1() {
    when(mockAttributeMapping.isArray(requestContext, ATTRIBUTE_ID1)).thenReturn(false);
    when(mockAttributeMapping.isPrimitive(TYPE_STRING)).thenReturn(true);
    when(mockAttributeMapping.getAttributeKind(requestContext, ATTRIBUTE_ID1))
        .thenReturn(Optional.of(TYPE_STRING));
    return when(mockAttributeMapping.getDocStorePathByAttributeId(requestContext, ATTRIBUTE_ID1))
        .thenReturn(Optional.of(EDS_COLUMN_NAME1))
        .getMock();
  }

  private EntityAttributeMapping mockMappingForAttributes1And2() {
    when(mockAttributeMapping.getAttributeKind(requestContext, ATTRIBUTE_ID2))
        .thenReturn(Optional.of(TYPE_STRING));
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

  static class ExceptionMessagePartialMatcher implements ArgumentMatcher<Exception> {

    private final String expectedMessage;

    ExceptionMessagePartialMatcher(String expectedMessage) {
      this.expectedMessage = expectedMessage;
    }

    @Override
    public boolean matches(Exception argument) {
      return argument.getMessage().contains(expectedMessage);
    }
  }
}
