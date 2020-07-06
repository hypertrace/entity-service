package org.hypertrace.entity.service.service;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.hypertrace.entity.service.client.config.EntityServiceTestConfig;
import org.hypertrace.entity.type.service.client.EntityTypeServiceClient;
import org.hypertrace.entity.type.service.v1.AttributeKind;
import org.hypertrace.entity.type.service.v1.AttributeType;
import org.hypertrace.entity.type.service.v1.EntityRelationshipType;
import org.hypertrace.entity.type.service.v1.EntityRelationshipTypeFilter;
import org.hypertrace.entity.type.service.v1.EntityType;
import org.hypertrace.entity.type.service.v1.EntityTypeFilter;
import org.hypertrace.entity.type.service.v1.MultiplicityKind;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test for testing {@link EntityTypeServiceClient}
 */
public class EntityTypeServiceTest {

  private static final String TENANT_ID =
      "__testTenant__" + EntityTypeServiceTest.class.getSimpleName();

  private static EntityTypeServiceClient client;

  @BeforeAll
  public static void setUp() {
    IntegrationTestServerUtil.startServices(new String[]{"entity-service"});
    Channel channel = ClientInterceptors.intercept(ManagedChannelBuilder.forAddress(
        EntityServiceTestConfig.getClientConfig().getHost(),
        EntityServiceTestConfig.getClientConfig().getPort()).usePlaintext().build());
    client = new EntityTypeServiceClient(channel);
  }

  @AfterAll
  public static void teardown() {
    IntegrationTestServerUtil.shutdownServices();
  }

  @BeforeEach
  public void setupMethod() {
    client.deleteEntityTypes(TENANT_ID, EntityTypeFilter.newBuilder().build());
    client.deleteEntityRelationshipTypes(TENANT_ID,
        EntityRelationshipTypeFilter.newBuilder().build());
  }

  @Test
  public void testUpsertAndQueryEntityType() {
    EntityType entityType1 = EntityType.newBuilder()
        .setName("EntityType1")
        .setTenantId(TENANT_ID)
        .addAttributeType(AttributeType.newBuilder()
            .setName("attr1")
            .setIdentifyingAttribute(true)
            .setValueKind(AttributeKind.TYPE_STRING)
            .build())
        .addAttributeType(AttributeType.newBuilder()
            .setName("someattr")
            .setValueKind(AttributeKind.TYPE_STRING)
            .build())
        .build();
    EntityType entityType2 = EntityType.newBuilder()
        .setName("EntityType2")
        .setTenantId(TENANT_ID)
        .addAttributeType(AttributeType.newBuilder()
            .setName("attr2")
            .setIdentifyingAttribute(true)
            .setValueKind(AttributeKind.TYPE_INT64)
            .build())
        .build();
    client.upsertEntityType(TENANT_ID, entityType1);
    client.upsertEntityType(TENANT_ID, entityType2);

    List<EntityType> entityTypes = client.getAllEntityTypes(TENANT_ID);
    Assertions.assertEquals(2, entityTypes.size());
    Assertions.assertTrue(entityTypes.containsAll(List.of(entityType1, entityType2)));

    entityTypes = client.queryEntityTypes(TENANT_ID, EntityTypeFilter.newBuilder()
        .addName(entityType1.getName())
        .build());
    Assertions.assertEquals(1, entityTypes.size());
    Assertions.assertTrue(entityTypes.contains(entityType1));
  }

  @Test
  public void testUpsertAndQueryEntityRelationshipType() {
    EntityRelationshipType entityRelationshipType1 = EntityRelationshipType.newBuilder()
        .setTenantId(TENANT_ID)
        .setName("ER1")
        .setMultiplicityKind(MultiplicityKind.ONE_TO_MANY)
        .setFromEntityType("fromType1")
        .setToEntityType("toType1")
        .build();
    EntityRelationshipType entityRelationshipType2 = EntityRelationshipType.newBuilder()
        .setTenantId(TENANT_ID)
        .setName("ER2")
        .setMultiplicityKind(MultiplicityKind.ONE_TO_ONE)
        .setFromEntityType("fromType2")
        .setToEntityType("toType2")
        .build();
    client.upsertEntityRelationshipType(TENANT_ID, entityRelationshipType1);
    client.upsertEntityRelationshipType(TENANT_ID, entityRelationshipType2);

    List<EntityRelationshipType> entityRelationshipTypes =
        client.getAllEntityRelationshipTypes(TENANT_ID);
    Assertions.assertEquals(2, entityRelationshipTypes.size());
    Assertions.assertTrue(entityRelationshipTypes.containsAll(
        List.of(entityRelationshipType1, entityRelationshipType2)));

    entityRelationshipTypes = client
        .queryRelationshipTypes(TENANT_ID, EntityRelationshipTypeFilter.newBuilder()
            .setFromEntityType(entityRelationshipType1.getFromEntityType())
            .build());
    Assertions.assertEquals(1, entityRelationshipTypes.size());
    Assertions.assertTrue(entityRelationshipTypes.contains(entityRelationshipType1));

    entityRelationshipTypes = client
        .queryRelationshipTypes(TENANT_ID, EntityRelationshipTypeFilter.newBuilder()
            .setToEntityType(entityRelationshipType1.getToEntityType())
            .build());
    Assertions.assertEquals(1, entityRelationshipTypes.size());
    Assertions.assertTrue(entityRelationshipTypes.contains(entityRelationshipType1));

    entityRelationshipTypes = client
        .queryRelationshipTypes(TENANT_ID, EntityRelationshipTypeFilter.newBuilder()
            .setMultiplicityKind(entityRelationshipType2.getMultiplicityKind())
            .build());
    Assertions.assertEquals(1, entityRelationshipTypes.size());
    Assertions.assertTrue(entityRelationshipTypes.contains(entityRelationshipType2));
  }
}
