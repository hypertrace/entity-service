package org.hypertrace.entity.data.service.rxclient;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc.EntityDataServiceImplBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntityDataCachingClientTest {

  final Entity defaultResponseEntity =
      Entity.newBuilder()
          .setEntityId("id-1")
          .setEntityType("type-1")
          .setEntityName("name-1")
          .build();

  @Mock RequestContext mockContext;

  @Mock EntityDataServiceImplBase mockDataService;

  EntityDataClient dataClient;

  Server grpcServer;
  ManagedChannel grpcChannel;
  Context grpcTestContext;
  List<Entity> possibleResponseEntities;
  Optional<Throwable> responseError;

  @BeforeEach
  void beforeEach() throws IOException {
    String uniqueName = InProcessServerBuilder.generateName();
    this.grpcServer =
        InProcessServerBuilder.forName(uniqueName)
            .directExecutor() // directExecutor is fine for unit tests
            .addService(this.mockDataService)
            .build()
            .start();
    this.grpcChannel = InProcessChannelBuilder.forName(uniqueName).directExecutor().build();
    this.dataClient = EntityDataClient.builder(this.grpcChannel).build();
    when(this.mockContext.getTenantId()).thenReturn(Optional.of("default tenant"));
    this.grpcTestContext = Context.current().withValue(RequestContext.CURRENT, this.mockContext);
    this.possibleResponseEntities = List.of(this.defaultResponseEntity);
    this.responseError = Optional.empty();
    doAnswer(
            invocation -> {
              StreamObserver<Entity> observer = invocation.getArgument(1, StreamObserver.class);
              Entity inputEntity = invocation.getArgument(0, Entity.class);
              responseError.ifPresentOrElse(
                  observer::onError,
                  () -> {
                    observer.onNext(
                        this.possibleResponseEntities.stream()
                            .filter(
                                entity -> entity.getEntityId().equals(inputEntity.getEntityId()))
                            .findFirst()
                            .orElse(inputEntity));
                    observer.onCompleted();
                  });
              return null;
            })
        .when(this.mockDataService)
        .upsert(any(), any());
  }

  @AfterEach
  void afterEach() {
    this.grpcServer.shutdownNow();
    this.grpcChannel.shutdownNow();
  }

  @Test
  void cachesConsecutiveCallsForSameEntity() throws Exception {
    Entity inputEntity = this.defaultResponseEntity.toBuilder().clearEntityName().build();
    assertSame(
        this.defaultResponseEntity,
        this.grpcTestContext.call(
            () -> this.dataClient.getOrCreateEntity(inputEntity).blockingGet()));

    verify(this.mockDataService, times(1)).upsert(any(), any());
    verifyNoMoreInteractions(this.mockDataService);
    assertSame(
        this.defaultResponseEntity,
        this.grpcTestContext.call(
            () -> this.dataClient.getOrCreateEntity(inputEntity).blockingGet()));
  }

  @Test
  void supportsMultipleConcurrentCacheKeys() throws Exception {
    Entity inputEntity = this.defaultResponseEntity.toBuilder().clearEntityName().build();
    Entity defaultRetrieved =
        this.grpcTestContext.call(
            () -> this.dataClient.getOrCreateEntity(inputEntity).blockingGet());
    assertSame(this.defaultResponseEntity, defaultRetrieved);
    verify(this.mockDataService, times(1)).upsert(any(), any());

    RequestContext otherMockContext = mock(RequestContext.class);
    when(otherMockContext.getTenantId()).thenReturn(Optional.of("other tenant"));
    Context otherGrpcContext =
        Context.current().withValue(RequestContext.CURRENT, otherMockContext);
    Entity otherEntityResponse =
        this.defaultResponseEntity.toBuilder().setEntityName("name-2").build();

    this.possibleResponseEntities = List.of(otherEntityResponse);

    Entity otherRetrieved =
        otherGrpcContext.call(() -> this.dataClient.getOrCreateEntity(inputEntity).blockingGet());
    assertSame(otherEntityResponse, otherRetrieved);
    assertNotSame(defaultRetrieved, otherRetrieved);
    verify(this.mockDataService, times(2)).upsert(any(), any());
    verifyNoMoreInteractions(this.mockDataService);

    assertSame(
        defaultRetrieved,
        this.grpcTestContext.call(
            () -> this.dataClient.getOrCreateEntity(inputEntity).blockingGet()));

    assertSame(
        otherRetrieved,
        otherGrpcContext.call(() -> this.dataClient.getOrCreateEntity(inputEntity).blockingGet()));
  }

  @Test
  void retriesOnError() throws Exception {
    Entity inputEntity = this.defaultResponseEntity.toBuilder().clearEntityName().build();
    this.responseError = Optional.of(new UnsupportedOperationException());

    assertThrows(
        StatusRuntimeException.class,
        () ->
            this.grpcTestContext.call(
                () -> this.dataClient.getOrCreateEntity(inputEntity).blockingGet()));
    verify(this.mockDataService, times(1)).upsert(any(), any());

    this.responseError = Optional.empty();
    assertSame(
        this.defaultResponseEntity,
        this.grpcTestContext.call(
            () -> this.dataClient.getOrCreateEntity(inputEntity).blockingGet()));
    verify(this.mockDataService, times(2)).upsert(any(), any());
  }

  @Test
  void hasConfigurableCacheSize() throws Exception {
    this.dataClient =
        EntityDataClient.builder(this.grpcChannel).withMaximumCacheContexts(1).build();

    RequestContext otherMockContext = mock(RequestContext.class);
    when(otherMockContext.getTenantId()).thenReturn(Optional.of("other tenant"));
    this.grpcTestContext.call(
        () -> this.dataClient.getOrCreateEntity(this.defaultResponseEntity).blockingGet());

    // This call should evict the original call
    Context.current()
        .withValue(RequestContext.CURRENT, otherMockContext)
        .call(() -> this.dataClient.getOrCreateEntity(this.defaultResponseEntity).blockingGet());

    // Rerunning this call now fire again, a third server call
    this.grpcTestContext.call(
        () -> this.dataClient.getOrCreateEntity(this.defaultResponseEntity).blockingGet());
    verify(this.mockDataService, times(3)).upsert(any(), any());
  }
}
