package org.hypertrace.entity.data.service.rxclient;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import io.reactivex.rxjava3.schedulers.TestScheduler;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc.EntityDataServiceImplBase;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityResponse;
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
  List<Entity> possibleResponseEntities;
  Optional<Throwable> responseError;
  TestScheduler testScheduler;

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
    this.possibleResponseEntities = List.of(this.defaultResponseEntity);
    this.responseError = Optional.empty();
    doAnswer(
            invocation -> {
              StreamObserver<MergeAndUpsertEntityResponse> observer =
                  invocation.getArgument(1, StreamObserver.class);
              Entity inputEntity =
                  invocation.getArgument(0, MergeAndUpsertEntityRequest.class).getEntity();
              responseError.ifPresentOrElse(
                  observer::onError,
                  () -> {
                    observer.onNext(
                        MergeAndUpsertEntityResponse.newBuilder()
                            .setEntity(
                                this.possibleResponseEntities.stream()
                                    .filter(
                                        entity ->
                                            entity.getEntityId().equals(inputEntity.getEntityId()))
                                    .findFirst()
                                    .orElse(inputEntity))
                            .build());
                    observer.onCompleted();
                  });
              return null;
            })
        .when(this.mockDataService)
        .mergeAndUpsertEntity(any(), any());
    this.testScheduler = new TestScheduler();
    RxJavaPlugins.setComputationSchedulerHandler(ignored -> testScheduler);
  }

  @AfterEach
  void afterEach() {
    // Reset
    RxJavaPlugins.setComputationSchedulerHandler(null);
    this.grpcServer.shutdownNow();
    this.grpcChannel.shutdownNow();
  }

  @Test
  void createOrUpdateCallsUpsert() {
    assertSame(
        this.defaultResponseEntity,
        this.dataClient
            .createOrUpdateEntity(
                mockContext, this.defaultResponseEntity, UpsertCondition.getDefaultInstance())
            .blockingGet());

    verify(this.mockDataService, times(1))
        .mergeAndUpsertEntity(
            eq(
                MergeAndUpsertEntityRequest.newBuilder()
                    .setEntity(this.defaultResponseEntity)
                    .setUpsertCondition(UpsertCondition.getDefaultInstance())
                    .build()),
            any());
  }

  @Test
  void createOrUpdateEventuallyThrottlesAndUsesLastProvidedValue() {
    this.possibleResponseEntities = Collections.emptyList(); // Just reflect entities in this test
    Entity firstEntity = this.defaultResponseEntity.toBuilder().setEntityName("first name").build();
    Entity secondEntity =
        this.defaultResponseEntity.toBuilder().setEntityName("second name").build();
    Entity thirdEntity = this.defaultResponseEntity.toBuilder().setEntityName("third name").build();

    this.dataClient.createOrUpdateEntityEventually(
        mockContext, firstEntity, UpsertCondition.getDefaultInstance(), Duration.ofMillis(1000));

    testScheduler.advanceTimeBy(300, TimeUnit.MILLISECONDS);

    this.dataClient.createOrUpdateEntityEventually(
        mockContext, secondEntity, UpsertCondition.getDefaultInstance(), Duration.ofMillis(200));

    testScheduler.advanceTimeBy(150, TimeUnit.MILLISECONDS);

    this.dataClient.createOrUpdateEntityEventually(
        mockContext, thirdEntity, UpsertCondition.getDefaultInstance(), Duration.ofMillis(5000));

    testScheduler.advanceTimeBy(49, TimeUnit.MILLISECONDS);

    // All 3 should complete at 500ms (we're currently at 499ms)
    verifyNoInteractions(this.mockDataService);

    testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

    verify(this.mockDataService, times(1))
        .mergeAndUpsertEntity(
            eq(
                MergeAndUpsertEntityRequest.newBuilder()
                    .setEntity(thirdEntity)
                    .setUpsertCondition(UpsertCondition.getDefaultInstance())
                    .build()),
            any());
  }
}
