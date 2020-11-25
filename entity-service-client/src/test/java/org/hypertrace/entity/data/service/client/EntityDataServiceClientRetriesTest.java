package org.hypertrace.entity.data.service.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.rpc.Code;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for {@link EntityDataServiceClient}
 */
@ExtendWith(MockitoExtension.class)
public class EntityDataServiceClientRetriesTest {

  @Mock
  EntityDataServiceGrpc.EntityDataServiceImplBase mockDataService;

  EntityDataServiceClient edsClient;
  Server grpcServer;
  ManagedChannel grpcChannel;

  @BeforeEach
  void beforeEach() throws IOException {
    String uniqueName = InProcessServerBuilder.generateName();
    this.grpcServer =
        InProcessServerBuilder.forName(uniqueName)
            .directExecutor() // directExecutor is fine for unit tests
            .addService(this.mockDataService)
            .build()
            .start();
    Map<String, Object> serviceConfig = Map.of(
        "methodConfig", Collections.<Object>singletonList(Map.of(
            "name", Collections.<Object>singletonList(Map.of("service", EntityDataServiceGrpc.getServiceDescriptor().getName())),
            "retryPolicy", Map.of(
                "maxAttempts", 3D,
                "initialBackoff", "0.1s",
                "maxBackoff", "5s",
                "backoffMultiplier", 2D,
                "retryableStatusCodes", Arrays.<Object>asList("UNAVAILABLE", "INTERNAL")
            )
            )
        )
    );
    this.grpcChannel = InProcessChannelBuilder.forName(uniqueName)
        .directExecutor()
        .enableRetry()
        .defaultServiceConfig(serviceConfig)
        .build();

    edsClient = new EntityDataServiceClient(this.grpcChannel);
  }

  @AfterEach
  void afterEach() {
    this.grpcServer.shutdownNow();
    this.grpcChannel.shutdownNow();
  }

  @Test
  public void testRetryForUnavailable() {
    doAnswer(
        invocation -> {
          StreamObserver<Entity> observer = invocation.getArgument(1, StreamObserver.class);
          observer.onError(new RuntimeException(new StatusRuntimeException(Status.UNAVAILABLE)));
          return null;
        })
        .when(this.mockDataService)
        .getById(any(), any());

    try {
      edsClient.getById("__default", "id1");
    } catch (RuntimeException e) {
      e.printStackTrace();
      Assertions.assertTrue(e.getCause() instanceof StatusRuntimeException);
      Assertions.assertEquals(Code.UNAVAILABLE.name(), ((StatusRuntimeException) e.getCause()).getStatus().getCode().name());
    }
    verify(this.mockDataService, times(3)).getById(any(), any());
  }

  @Test
  public void testRetryForInternalError() {
    doAnswer(
        invocation -> {
          StreamObserver<Entity> observer = invocation.getArgument(1, StreamObserver.class);
          observer.onError(new RuntimeException(new StatusRuntimeException(Status.INTERNAL)));
          return null;
        })
        .when(this.mockDataService)
        .getById(any(), any());

    try {
      edsClient.getById("__default", "id1");
    } catch (RuntimeException e) {
      e.printStackTrace();
      Assertions.assertTrue(e.getCause() instanceof StatusRuntimeException);
      Assertions.assertEquals(Code.INTERNAL.name(), ((StatusRuntimeException) e.getCause()).getStatus().getCode().name());
    }
    verify(this.mockDataService, times(3)).getById(any(), any());
  }
}
