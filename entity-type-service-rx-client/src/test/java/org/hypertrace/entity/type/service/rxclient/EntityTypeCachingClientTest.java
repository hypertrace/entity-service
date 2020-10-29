package org.hypertrace.entity.type.service.rxclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.reactivex.rxjava3.core.Single;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.type.service.v2.EntityType;
import org.hypertrace.entity.type.service.v2.EntityTypeServiceGrpc.EntityTypeServiceImplBase;
import org.hypertrace.entity.type.service.v2.QueryEntityTypesResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntityTypeCachingClientTest {

  EntityType type1 =
      EntityType.newBuilder()
          .setName("first")
          .setAttributeScope("first_scope")
          .setIdAttributeKey("first_id")
          .setNameAttributeKey("first_name")
          .build();
  EntityType type2 =
      EntityType.newBuilder()
          .setName("second")
          .setAttributeScope("second_scope")
          .setIdAttributeKey("second_id")
          .setNameAttributeKey("second_name")
          .build();

  @Mock RequestContext mockContext;

  @Mock EntityTypeServiceImplBase mockTypeService;

  EntityTypeClient typeClient;

  Server grpcServer;
  ManagedChannel grpcChannel;
  Context grpcTestContext;
  List<EntityType> responseTypes;
  Optional<Throwable> responseError;

  @BeforeEach
  void beforeEach() throws IOException {
    String uniqueName = InProcessServerBuilder.generateName();
    this.grpcServer =
        InProcessServerBuilder.forName(uniqueName)
            .directExecutor() // directExecutor is fine for unit tests
            .addService(this.mockTypeService)
            .build()
            .start();
    this.grpcChannel = InProcessChannelBuilder.forName(uniqueName).directExecutor().build();
    this.typeClient = EntityTypeClient.builder(this.grpcChannel).build();
    when(this.mockContext.getTenantId()).thenReturn(Optional.of("default tenant"));
    this.grpcTestContext = Context.current().withValue(RequestContext.CURRENT, this.mockContext);
    this.responseTypes = List.of(this.type1, this.type2);
    this.responseError = Optional.empty();
    doAnswer(
            invocation -> {
              StreamObserver<QueryEntityTypesResponse> observer =
                  invocation.getArgument(1, StreamObserver.class);
              responseError.ifPresentOrElse(
                  observer::onError,
                  () -> {
                    observer.onNext(
                        QueryEntityTypesResponse.newBuilder()
                            .addAllEntityType(this.responseTypes)
                            .build());
                    observer.onCompleted();
                  });
              return null;
            })
        .when(this.mockTypeService)
        .queryEntityTypes(any(), any());
  }

  @AfterEach
  void afterEach() {
    this.grpcServer.shutdownNow();
    this.grpcChannel.shutdownNow();
  }

  @Test
  void cachesConsecutiveGetAllCallsInSameContext() throws Exception {
    assertSame(
        this.type1, this.grpcTestContext.call(() -> this.typeClient.get("first").blockingGet()));
    verify(this.mockTypeService, times(1)).queryEntityTypes(any(), any());
    verifyNoMoreInteractions(this.mockTypeService);
    assertSame(
        this.type2, this.grpcTestContext.call(() -> this.typeClient.get("second").blockingGet()));
  }

  @Test
  void throwsErrorIfNoKeyMatch() {
    assertThrows(
        NoSuchElementException.class,
        () -> this.grpcTestContext.run(() -> this.typeClient.get("third").blockingGet()));
  }

  @Test
  void lazilyFetchesOnSubscribe() throws Exception {
    Single<EntityType> type = this.grpcTestContext.call(() -> this.typeClient.get("first"));
    verifyNoInteractions(this.mockTypeService);
    type.subscribe();
    verify(this.mockTypeService, times(1)).queryEntityTypes(any(), any());
  }

  @Test
  void supportsMultipleConcurrentCacheKeys() throws Exception {
    EntityType defaultRetrieved =
        this.grpcTestContext.call(() -> this.typeClient.get("first").blockingGet());
    assertSame(this.type1, defaultRetrieved);
    verify(this.mockTypeService, times(1)).queryEntityTypes(any(), any());

    RequestContext otherMockContext = mock(RequestContext.class);
    when(otherMockContext.getTenantId()).thenReturn(Optional.of("other tenant"));
    Context otherGrpcContext =
        Context.current().withValue(RequestContext.CURRENT, otherMockContext);
    EntityType otherContextType = EntityType.newBuilder(this.type1).build();

    this.responseTypes = List.of(otherContextType);

    EntityType otherRetrieved =
        otherGrpcContext.call(() -> this.typeClient.get("first").blockingGet());
    assertSame(otherContextType, otherRetrieved);
    assertNotSame(defaultRetrieved, otherRetrieved);
    verify(this.mockTypeService, times(2)).queryEntityTypes(any(), any());
    verifyNoMoreInteractions(this.mockTypeService);

    assertSame(
        defaultRetrieved,
        this.grpcTestContext.call(() -> this.typeClient.get("first").blockingGet()));

    assertSame(
        otherRetrieved, otherGrpcContext.call(() -> this.typeClient.get("first").blockingGet()));
  }

  @Test
  void retriesOnError() throws Exception {
    this.responseError = Optional.of(new UnsupportedOperationException());

    assertThrows(
        StatusRuntimeException.class,
        () -> this.grpcTestContext.call(() -> this.typeClient.get("first").blockingGet()));
    verify(this.mockTypeService, times(1)).queryEntityTypes(any(), any());

    this.responseError = Optional.empty();
    assertSame(
        this.type1, this.grpcTestContext.call(() -> this.typeClient.get("first").blockingGet()));
    verify(this.mockTypeService, times(2)).queryEntityTypes(any(), any());
  }

  @Test
  void hasConfigurableCacheSize() throws Exception {
    this.typeClient =
        EntityTypeClient.builder(this.grpcChannel).withMaximumCacheContexts(1).build();

    RequestContext otherMockContext = mock(RequestContext.class);
    when(otherMockContext.getTenantId()).thenReturn(Optional.of("other tenant"));
    this.grpcTestContext.call(() -> this.typeClient.get("first").blockingGet());

    // This call should evict the original call
    Context.current()
        .withValue(RequestContext.CURRENT, otherMockContext)
        .call(() -> this.typeClient.get("first").blockingGet());

    // Rerunning this call now fire again, a third server call
    this.grpcTestContext.call(() -> this.typeClient.get("first").blockingGet());
    verify(this.mockTypeService, times(3)).queryEntityTypes(any(), any());
  }

  @Test
  void getsAllAttributesInScope() throws Exception {
    assertIterableEquals(
        this.responseTypes,
        this.grpcTestContext.call(() -> this.typeClient.getAll().blockingIterable()));

    verifyNoMoreInteractions(this.mockTypeService);

    assertIterableEquals(
        this.responseTypes,
        this.grpcTestContext.call(() -> this.typeClient.getAll().blockingIterable()));
  }
}
