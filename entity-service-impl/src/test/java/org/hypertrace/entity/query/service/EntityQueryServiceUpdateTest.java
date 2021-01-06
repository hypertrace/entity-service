package org.hypertrace.entity.query.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.EntityUpdateRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.LiteralConstant.Builder;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.SetAttribute;
import org.hypertrace.entity.query.service.v1.UpdateOperation;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.service.util.DocStoreJsonFormat;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

public class EntityQueryServiceUpdateTest {

  private final Map<String, Map<String, String>> attributeFqnMaps = new HashMap<>();
  private static final String TEST_ENTITY_NAME = "TEST_ENTITY";

  public EntityQueryServiceUpdateTest() {
    // Init attribute FQN mapping needed for tests here
    Map<String, String> entityAttributeFqnMap = new HashMap<>();
    entityAttributeFqnMap.put("Entity.id", "attributes.entity_id");
    entityAttributeFqnMap.put("Entity.status", "attributes.status");

    attributeFqnMaps.put(TEST_ENTITY_NAME, entityAttributeFqnMap);
  }

  @Test
  public void noTenantId() throws Exception {
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);

    Context.current()
        .withValue(RequestContext.CURRENT, mock(RequestContext.class))
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(mockEntitiesCollection(), attributeFqnMaps);

              eqs.update(null, mockResponseObserver);

              verify(mockResponseObserver, times(1))
                  .onError(argThat(
                      new ExceptionMessageMatcher("Tenant id is missing in the request.")));
              return null;
            });
  }

  @Test
  public void noEntityType() throws Exception {
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);

    Context.current()
        .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(mockEntitiesCollection(), attributeFqnMaps);

              eqs.update(EntityUpdateRequest.newBuilder().build(), mockResponseObserver);

              verify(mockResponseObserver, times(1))
                  .onError(argThat(
                      new ExceptionMessageMatcher("Entity type is missing in the request.")));
              return null;
            });
  }

  @Test
  public void noEntityId() throws Exception {
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);

    Context.current()
        .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(mockEntitiesCollection(), attributeFqnMaps);

              eqs.update(EntityUpdateRequest.newBuilder()
                  .setEntityType(TEST_ENTITY_NAME)
                  .build(), mockResponseObserver);

              verify(mockResponseObserver, times(1))
                  .onError(argThat(
                      new ExceptionMessageMatcher("Entity IDs are missing in the request.")));
              return null;
            });
  }

  @Test
  public void noOperation() throws Exception {
    StreamObserver<ResultSetChunk> mockResponseObserver = mock(StreamObserver.class);

    Context.current()
        .withValue(RequestContext.CURRENT, mockRequestContextWithTenantId())
        .call(
            () -> {
              EntityQueryServiceImpl eqs =
                  new EntityQueryServiceImpl(mockEntitiesCollection(), attributeFqnMaps);

              eqs.update(EntityUpdateRequest.newBuilder()
                  .setEntityType(TEST_ENTITY_NAME)
                  .addEntityIds("entity-id-1")
                  .build(), mockResponseObserver);

              verify(mockResponseObserver, times(1))
                  .onError(argThat(
                      new ExceptionMessageMatcher("Operation is missing in the request.")));
              return null;
            });
  }

  @Test
  public void updateSuccess() throws Exception {
    Collection mockEntitiesCollection = mockEntitiesCollection();

    Builder newStatus =
        LiteralConstant.newBuilder()
            .setValue(Value.newBuilder().setValueType(ValueType.STRING).setString("NEW_STATUS"));

    EntityUpdateRequest updateRequest =
        EntityUpdateRequest.newBuilder()
            .setEntityType(TEST_ENTITY_NAME)
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
                  new EntityQueryServiceImpl(mockEntitiesCollection, attributeFqnMaps);
              eqs.update(updateRequest, mockResponseObserver);
              return null;
            });

    verify(mockEntitiesCollection, times(1))
        .updateSubDoc(
            eq(new SingleValueKey("tenant1", "entity-id-1")),
            eq("attributes.status"),
            eq(new JSONDocument(DocStoreJsonFormat.printer().print(newStatus))));
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
