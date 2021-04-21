package org.hypertrace.entity.type.service.v2;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.hypertrace.entity.service.client.config.EntityServiceTestConfig;
import org.hypertrace.entity.type.client.EntityTypeServiceClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Integration test for testing {@link EntityTypeServiceImpl} */
public class EntityTypeServiceTest {
  private static final String TENANT_ID =
      "__testTenant__" + EntityTypeServiceTest.class.getSimpleName();

  private static EntityTypeServiceClient client;
  private static final int CONTAINER_STARTUP_ATTEMPTS = 5;
  private static Network network;
  private static GenericContainer<?> mongo;

  @BeforeAll
  public static void setUp() throws Exception {
    network = Network.newNetwork();
    mongo = new GenericContainer<>(DockerImageName.parse("hypertrace/mongodb:main"))
        .withNetwork(network)
        .withNetworkAliases("mongo")
        .withExposedPorts(27017)
        .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
        .waitingFor(Wait.forLogMessage(".*waiting for connections on port 27017.*", 1));
    mongo.start();

    withEnvironmentVariable(
        "MONGO_HOST", mongo.getHost())
        .and("MONGO_PORT",  mongo.getMappedPort(27017).toString())
        .execute(() ->
            IntegrationTestServerUtil.startServices(new String[] {"entity-service"}));
    Channel channel =
        ClientInterceptors.intercept(
            ManagedChannelBuilder.forAddress(
                    EntityServiceTestConfig.getClientConfig().getHost(),
                    EntityServiceTestConfig.getClientConfig().getPort())
                .usePlaintext()
                .build());
    client = new EntityTypeServiceClient(channel);
  }

  @AfterAll
  public static void teardown() {
    IntegrationTestServerUtil.shutdownServices();
    mongo.stop();
  }

  @BeforeEach
  public void setupMethod() {
    client.deleteAllEntityTypes(TENANT_ID);
  }

  @Test()
  public void testInvalidEntityTypeUpsert() {
    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          EntityType entityType1 = EntityType.newBuilder().setName("API").build();
          client.upsertEntityType(TENANT_ID, entityType1);
        });
  }

  @Test
  public void testAllEntityTypeMethods() {
    EntityType entityType1 =
        EntityType.newBuilder()
            .setName("API")
            .setAttributeScope("API")
            .setIdAttributeKey("id")
            .setNameAttributeKey("name")
            .build();
    EntityType entityType2 =
        EntityType.newBuilder()
            .setName("SERVICE")
            .setAttributeScope("SERVICE")
            .setIdAttributeKey("id")
            .setNameAttributeKey("name")
            .build();
    client.upsertEntityType(TENANT_ID, entityType1);
    client.upsertEntityType(TENANT_ID, entityType2);

    List<EntityType> entityTypes = client.getAllEntityTypes(TENANT_ID);
    Assertions.assertEquals(2, entityTypes.size());
    Assertions.assertTrue(entityTypes.containsAll(List.of(entityType1, entityType2)));

    entityTypes = client.queryEntityTypes(TENANT_ID, List.of(entityType1.getName()));
    Assertions.assertEquals(1, entityTypes.size());
    Assertions.assertTrue(entityTypes.contains(entityType1));

    // Lookup unknown entity type and assert empty result.
    entityTypes = client.queryEntityTypes(TENANT_ID, List.of("unknown"));
    Assertions.assertTrue(entityTypes.isEmpty());

    // Delete one and verify there is only one left.
    client.deleteEntityTypes(TENANT_ID, List.of("API"));

    // Query all types
    entityTypes = client.getAllEntityTypes(TENANT_ID);
    Assertions.assertEquals(1, entityTypes.size());
    Assertions.assertTrue(entityTypes.contains(entityType2));
  }
}
