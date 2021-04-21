package org.hypertrace.entity.service.service;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import org.hypertrace.core.grpcutils.client.RequestContextAsCreds;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.hypertrace.entity.constants.v1.BackendAttribute;
import org.hypertrace.entity.constants.v1.CommonAttribute;
import org.hypertrace.entity.data.service.client.EntityDataServiceClient;
import org.hypertrace.entity.data.service.v1.AttributeFilter;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.AttributeValueList;
import org.hypertrace.entity.data.service.v1.AttributeValueMap;
import org.hypertrace.entity.data.service.v1.ByIdRequest;
import org.hypertrace.entity.data.service.v1.ByTypeAndIdentifyingAttributes;
import org.hypertrace.entity.data.service.v1.EnrichedEntities;
import org.hypertrace.entity.data.service.v1.EnrichedEntity;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc.EntityDataServiceBlockingStub;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate.PredicateOperator;
import org.hypertrace.entity.data.service.v1.Operator;
import org.hypertrace.entity.data.service.v1.OrderByExpression;
import org.hypertrace.entity.data.service.v1.Query;
import org.hypertrace.entity.data.service.v1.SortOrder;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.client.config.EntityServiceClientConfig;
import org.hypertrace.entity.service.client.config.EntityServiceTestConfig;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.service.constants.EntityServiceConstants;
import org.hypertrace.entity.type.client.EntityTypeServiceClient;
import org.hypertrace.entity.type.service.v1.AttributeKind;
import org.hypertrace.entity.type.service.v1.AttributeType;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Test case for {@link EntityDataServiceClient} */
public class EntityDataServiceTest {
  private static final String TENANT_ID =
      "__testTenant__" + EntityDataServiceTest.class.getSimpleName();
  private static final String TEST_ENTITY_TYPE_V2 = "TEST_ENTITY";
  private static EntityDataServiceClient entityDataServiceClient;
  private static ManagedChannel channel;
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
    EntityServiceClientConfig esConfig = EntityServiceTestConfig.getClientConfig();
    channel =
        ManagedChannelBuilder.forAddress(esConfig.getHost(), esConfig.getPort())
            .usePlaintext()
            .build();
    entityDataServiceClient = new EntityDataServiceClient(channel);
    setupEntityTypes(channel, TENANT_ID);
  }

  @AfterAll
  public static void teardown() {
    channel.shutdown();
    IntegrationTestServerUtil.shutdownServices();
    mongo.stop();
  }

  private static void setupEntityTypes(Channel channel, String tenant) {
    org.hypertrace.entity.type.service.client.EntityTypeServiceClient entityTypeServiceV1Client =
        new org.hypertrace.entity.type.service.client.EntityTypeServiceClient(channel);
    EntityTypeServiceClient entityTypeServiceV2Client = new EntityTypeServiceClient(channel);
    entityTypeServiceV1Client.upsertEntityType(
        tenant,
        org.hypertrace.entity.type.service.v1.EntityType.newBuilder()
            .setName(EntityType.K8S_POD.name())
            .addAttributeType(
                AttributeType.newBuilder()
                    .setName(EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID))
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setIdentifyingAttribute(true)
                    .build())
            .build());
    entityTypeServiceV1Client.upsertEntityType(
        tenant,
        org.hypertrace.entity.type.service.v1.EntityType.newBuilder()
            .setName(EntityType.BACKEND.name())
            .addAttributeType(
                AttributeType.newBuilder()
                    .setName(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_HOST))
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setIdentifyingAttribute(true)
                    .build())
            .addAttributeType(
                AttributeType.newBuilder()
                    .setName(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PORT))
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setIdentifyingAttribute(true)
                    .build())
            .addAttributeType(
                AttributeType.newBuilder()
                    .setName(EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL))
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setIdentifyingAttribute(true)
                    .build())
            .build());

    entityTypeServiceV2Client.upsertEntityType(
        tenant,
        org.hypertrace.entity.type.service.v2.EntityType.newBuilder()
            .setName(TEST_ENTITY_TYPE_V2)
            .setAttributeScope(TEST_ENTITY_TYPE_V2)
            .setIdAttributeKey("id")
            .setNameAttributeKey("name")
            .build());
  }

  @Test
  public void testCreateAndGetEntity() {
    Entity entity =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .build();
    upsertAndVerify(entity);

    Entity backendEntity =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.BACKEND.name())
            .setEntityName("Some Backend")
            .putIdentifyingAttributes(
                EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_HOST),
                generateRandomUUIDAttrValue())
            .putIdentifyingAttributes(
                EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PORT),
                generateRandomUUIDAttrValue())
            .putIdentifyingAttributes(
                EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PROTOCOL),
                generateRandomUUIDAttrValue())
            .putAttributes(
                EntityConstants.getValue(BackendAttribute.BACKEND_ATTRIBUTE_PATH),
                generateRandomUUIDAttrValue())
            .build();
    upsertAndVerify(backendEntity);
  }

  @Test
  public void testCreateWithIdentifyingAttributes() {
    AttributeValue randomUUIDAttrValue = generateRandomUUIDAttrValue();
    Entity entity =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                randomUUIDAttrValue)
            .build();
    Entity createdEntity = entityDataServiceClient.upsert(entity);
    assertNotNull(createdEntity.getEntityId());

    Entity entity1 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service 1")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                randomUUIDAttrValue)
            .build();
    Entity createdEntity1 = entityDataServiceClient.upsert(entity1);
    // Should be same entity since identifying attributes are the same
    assertEquals(createdEntity.getEntityId(), createdEntity1.getEntityId());
  }

  @Test
  public void testCreateV1EntityTypeWithoutIdentifyingAttributes() {
    Entity entity =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service")
            .build();

    StatusRuntimeException exception =
        assertThrows(StatusRuntimeException.class, () -> entityDataServiceClient.upsert(entity));
    assertEquals(Code.INTERNAL, exception.getStatus().getCode());
    assertTrue(exception.getStatus().getDescription().contains("expected identifying attributes"));
  }

  @Test
  public void testCreateV2EntityTypeWithoutIdentifyingAttributes() {
    Entity firstInputEntity =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(TEST_ENTITY_TYPE_V2)
            .setEntityId(UUID.randomUUID().toString())
            .setEntityName("Test entity v2")
            .putAttributes(
                "foo",
                AttributeValue.newBuilder().setValue(Value.newBuilder().setString("foo1")).build())
            .build();
    Entity firstCreatedEntity = entityDataServiceClient.upsert(firstInputEntity);
    assertNotSame(firstInputEntity, firstCreatedEntity);
    assertEquals(firstInputEntity, firstCreatedEntity);

    Entity secondInputEntity = firstInputEntity.toBuilder().clearAttributes().build();

    Entity secondCreatedEntity = entityDataServiceClient.upsert(secondInputEntity);

    assertEquals(firstCreatedEntity, secondCreatedEntity);
  }

  @Test
  public void testDeleteEntity() {
    Entity entity =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .build();
    Entity createdEntity = entityDataServiceClient.upsert(entity);
    assertNotNull(createdEntity);
    assertNotNull(createdEntity.getEntityId().trim());

    entityDataServiceClient.delete(createdEntity.getTenantId(), createdEntity.getEntityId());

    Entity readEntity = entityDataServiceClient.getById(TENANT_ID, createdEntity.getEntityId());
    Assertions.assertNull(readEntity);
  }

  @Test
  public void testUpdateEntity() {
    Entity entity =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .build();
    Entity createdEntity = entityDataServiceClient.upsert(entity);
    assertNotNull(createdEntity);
    assertNotNull(createdEntity.getEntityId().trim());

    Entity updatedEntity =
        Entity.newBuilder(createdEntity).setEntityName("Updated Service").build();
    updatedEntity = entityDataServiceClient.upsert(updatedEntity);
    assertEquals("Updated Service", updatedEntity.getEntityName());
  }

  @Test
  public void testEntityGetByTypeAndIdentifyingProperties() {
    AttributeValue identifyingAttrValue =
        AttributeValue.newBuilder()
            .setValue(Value.newBuilder().setString("value1-" + System.nanoTime()).build())
            .build();
    Entity entity =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                identifyingAttrValue)
            .build();
    Entity createdEntity = entityDataServiceClient.upsert(entity);
    assertNotNull(createdEntity);
    assertNotNull(createdEntity.getEntityId().trim());

    ByTypeAndIdentifyingAttributes byTypeAndIdentifyingAttributes =
        ByTypeAndIdentifyingAttributes.newBuilder()
            .setEntityType(EntityType.K8S_POD.name())
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                identifyingAttrValue)
            .build();
    Entity foundEntity =
        entityDataServiceClient.getByTypeAndIdentifyingAttributes(
            TENANT_ID, byTypeAndIdentifyingAttributes);
    assertEquals(createdEntity, foundEntity);
  }

  @Test
  public void testEntityQuery() {
    Entity entity1 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service 1")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .build();
    Entity createdEntity1 = entityDataServiceClient.upsert(entity1);
    assertNotNull(createdEntity1);
    assertNotNull(createdEntity1.getEntityId().trim());

    Entity entity2 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service 2")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .build();
    Entity createdEntity2 = entityDataServiceClient.upsert(entity2);
    assertNotNull(createdEntity2);
    assertNotNull(createdEntity2.getEntityId().trim());

    Query entityTypeQuery = Query.newBuilder().setEntityType(EntityType.K8S_POD.name()).build();

    // Query all entities of type SERVICE
    List<Entity> entitiesList = entityDataServiceClient.query(TENANT_ID, entityTypeQuery);
    assertTrue(entitiesList.size() > 1);

    Query entityLimitQuery =
        Query.newBuilder().setEntityType(EntityType.K8S_POD.name()).setLimit(1).build();
    entitiesList = entityDataServiceClient.query(TENANT_ID, entityLimitQuery);
    assertEquals(1, entitiesList.size());

    Query entityOffsetQuery =
        Query.newBuilder()
            .setEntityType(EntityType.K8S_POD.name())
            .setLimit(1)
            .setOffset(1)
            .build();
    List<Entity> entityWithOffset = entityDataServiceClient.query(TENANT_ID, entityOffsetQuery);
    assertEquals(1, entityWithOffset.size());
    assertNotEquals(entitiesList.get(0).getEntityId(), entityWithOffset.get(0).getEntityId());

    // Query specific entity
    Query entityTypeAndIdQuery =
        Query.newBuilder()
            .addEntityId(createdEntity1.getEntityId())
            .setEntityType(EntityType.K8S_POD.name())
            .build();

    entitiesList = entityDataServiceClient.query(TENANT_ID, entityTypeAndIdQuery);
    Entity foundEntity = entitiesList.get(0);
    assertEquals(createdEntity1, foundEntity);

    // Query for entity that doesn't exist
    Query randomEntityIdQuery =
        Query.newBuilder()
            .addEntityId(UUID.randomUUID().toString())
            .setEntityType(EntityType.K8S_POD.name())
            .build();
    entitiesList = entityDataServiceClient.query(TENANT_ID, randomEntityIdQuery);
    assertTrue(entitiesList.isEmpty());
  }

  @Test
  public void testEntityNonAttributeQuery() {
    Entity entity1 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service 1")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .build();
    Entity createdEntity1 = entityDataServiceClient.upsert(entity1);
    assertNotNull(createdEntity1);
    assertFalse(createdEntity1.getEntityId().trim().isEmpty());

    Entity entity2 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service 2")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .build();
    Entity createdEntity2 = entityDataServiceClient.upsert(entity2);
    assertNotNull(createdEntity2);
    assertFalse(createdEntity2.getEntityId().trim().isEmpty());

    long afterCreatedTime = Instant.now().toEpochMilli();
    Query createTimeQuery =
        Query.newBuilder()
            .setEntityType(EntityType.K8S_POD.name())
            .setFilter(
                AttributeFilter.newBuilder()
                    .setOperator(Operator.LT)
                    .setName("createdTime")
                    .setAttributeValue(
                        AttributeValue.newBuilder()
                            .setValue(Value.newBuilder().setLong(afterCreatedTime).build()))
                    .build())
            .build();

    // Query all entities that created time is less than now
    List<Entity> entitiesList = entityDataServiceClient.query(TENANT_ID, createTimeQuery);
    assertTrue(entitiesList.size() > 1);
  }

  @Test
  public void testEntityQueryAttributeFiltering() {
    long timeBeforeQuery = System.currentTimeMillis();
    String stringRandomizer = UUID.randomUUID().toString();
    Entity entity =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("value1").build())
                    .build())
            .putAttributes(
                "simpleValue" + "-" + stringRandomizer,
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("StringValue").build())
                    .build())
            .putAttributes(
                "listValue" + "-" + stringRandomizer,
                AttributeValue.newBuilder()
                    .setValueList(
                        AttributeValueList.newBuilder()
                            .addValues(
                                AttributeValue.newBuilder()
                                    .setValue(Value.newBuilder().setString("item1").build())
                                    .build())
                            .addValues(
                                AttributeValue.newBuilder()
                                    .setValue(Value.newBuilder().setString("item2").build())
                                    .build())
                            .build())
                    .build())
            .putAttributes(
                "mapValue" + "-" + stringRandomizer,
                AttributeValue.newBuilder()
                    .setValueMap(
                        AttributeValueMap.newBuilder()
                            .putValues(
                                "nestedKey",
                                AttributeValue.newBuilder()
                                    .setValue(Value.newBuilder().setString("nestedValue").build())
                                    .build())
                            .build())
                    .build())
            .build();
    Entity createdEntity = entityDataServiceClient.upsert(entity);
    assertNotNull(createdEntity.getEntityId());

    AttributeFilter stringFilter =
        AttributeFilter.newBuilder()
            .setName(EntityConstants.attributeMapPathFor("simpleValue" + "-" + stringRandomizer))
            .setOperator(Operator.EQ)
            .setAttributeValue(
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("StringValue").build())
                    .build())
            .build();
    List<Entity> entities1 =
        entityDataServiceClient.query(
            TENANT_ID, Query.newBuilder().setFilter(stringFilter).build());
    Entity foundEntity1 = entities1.get(0);
    assertEquals(createdEntity, foundEntity1);

    // filter by createdTime, which is treated as attribute filtering because it should be able
    // to support different operations like regular attribtues
    AttributeFilter createTimeFilter =
        AttributeFilter.newBuilder()
            .setName(EntityServiceConstants.ENTITY_CREATED_TIME)
            .setOperator(Operator.GT)
            .setAttributeValue(
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setLong(timeBeforeQuery).build())
                    .build())
            .build();
    List<Entity> entities11 =
        entityDataServiceClient.query(
            TENANT_ID, Query.newBuilder().setFilter(createTimeFilter).build());
    Entity foundEntity11 = entities11.get(0);
    assertEquals(createdEntity, foundEntity11);

    AttributeFilter nestedFilter =
        AttributeFilter.newBuilder()
            .setName(EntityConstants.attributeMapPathFor("mapValue" + "-" + stringRandomizer))
            .setOperator(Operator.EQ)
            .setAttributeValue(
                AttributeValue.newBuilder()
                    .setValueMap(
                        AttributeValueMap.newBuilder()
                            .putValues(
                                "nestedKey",
                                AttributeValue.newBuilder()
                                    .setValue(Value.newBuilder().setString("nestedValue").build())
                                    .build())
                            .build())
                    .build())
            .build();
    List<Entity> entities2 =
        entityDataServiceClient.query(
            TENANT_ID, Query.newBuilder().setFilter(nestedFilter).build());
    Entity foundEntity2 = entities2.get(0);
    assertEquals(createdEntity, foundEntity2);

    AttributeFilter listFilter =
        AttributeFilter.newBuilder()
            .setName(EntityConstants.attributeMapPathFor("listValue" + "-" + stringRandomizer))
            .setOperator(Operator.EQ)
            .setAttributeValue(
                AttributeValue.newBuilder()
                    .setValueList(
                        AttributeValueList.newBuilder()
                            .addValues(
                                AttributeValue.newBuilder()
                                    .setValue(Value.newBuilder().setString("item1").build())
                                    .build())
                            .addValues(
                                AttributeValue.newBuilder()
                                    .setValue(Value.newBuilder().setString("item2").build())
                                    .build())
                            .build())
                    .build())
            .build();
    List<Entity> entities3 =
        entityDataServiceClient.query(TENANT_ID, Query.newBuilder().setFilter(listFilter).build());
    Entity foundEntity3 = entities3.get(0);
    assertEquals(createdEntity, foundEntity3);

    AttributeFilter listInFilter =
        AttributeFilter.newBuilder()
            .setName(EntityConstants.attributeMapPathFor("listValue" + "-" + stringRandomizer))
            .setOperator(Operator.CONTAINS)
            .setAttributeValue(
                AttributeValue.newBuilder()
                    .setValueList(
                        AttributeValueList.newBuilder()
                            .addValues(
                                AttributeValue.newBuilder()
                                    .setValue(Value.newBuilder().setString("item1").build())
                                    .build())
                            .build())
                    .build())
            .build();
    List<Entity> entities4 =
        entityDataServiceClient.query(
            TENANT_ID, Query.newBuilder().setFilter(listInFilter).build());
    Entity foundEntity4 = entities4.get(0);
    assertEquals(createdEntity, foundEntity4);
  }

  @Test
  public void testEntityQueryAttributeWithExistsFiltering() {
    String stringRandomizer1 = UUID.randomUUID().toString();
    Entity entity1 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("value1").build())
                    .build())
            .putAttributes(
                "simpleValue1" + "-" + stringRandomizer1,
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("StringValue1").build())
                    .build())
            .build();
    Entity createdEntity1 = entityDataServiceClient.upsert(entity1);
    assertNotNull(createdEntity1.getEntityId());

    String stringRandomizer2 = UUID.randomUUID().toString();
    Entity entity2 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("value2").build())
                    .build())
            .putAttributes(
                "simpleValue2" + "-" + stringRandomizer2,
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("StringValue2").build())
                    .build())
            .putAttributes(
                "test" + "-" + stringRandomizer2,
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("test").build())
                    .build())
            .build();
    Entity createdEntity2 = entityDataServiceClient.upsert(entity2);
    assertNotNull(createdEntity2.getEntityId());

    // test for exists operator
    AttributeFilter existsFilter =
        AttributeFilter.newBuilder()
            .setName(EntityConstants.attributeMapPathFor("simpleValue1" + "-" + stringRandomizer1))
            .setOperator(Operator.EXISTS)
            .build();
    List<Entity> entities =
        entityDataServiceClient.query(
            TENANT_ID, Query.newBuilder().setFilter(existsFilter).build());

    assertEquals(1, entities.size());
    assertEquals(createdEntity1, entities.get(0));

    // test for not-exists operator
    AttributeFilter notExistsFilter =
        AttributeFilter.newBuilder()
            .setName(EntityConstants.attributeMapPathFor("simpleValue3"))
            .setOperator(Operator.NOT_EXISTS)
            .build();

    entities =
        entityDataServiceClient.query(
            TENANT_ID, Query.newBuilder().setFilter(notExistsFilter).build());

    assertTrue(entities.size() > 0);

    // test with AND operator
    AttributeFilter eqFilter =
        AttributeFilter.newBuilder()
            .setName(EntityConstants.attributeMapPathFor("test" + "-" + stringRandomizer2))
            .setOperator(Operator.EQ)
            .setAttributeValue(
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("test").build())
                    .build())
            .build();

    existsFilter =
        AttributeFilter.newBuilder()
            .setName(EntityConstants.attributeMapPathFor("simpleValue2" + "-" + stringRandomizer2))
            .setOperator(Operator.EXISTS)
            .build();

    AttributeFilter andFilter =
        AttributeFilter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(eqFilter)
            .addChildFilter(existsFilter)
            .build();

    entities =
        entityDataServiceClient.query(TENANT_ID, Query.newBuilder().setFilter(andFilter).build());

    assertEquals(1, entities.size());
    assertEquals(createdEntity2, entities.get(0));

    // exists with attribute value - discard the value
    existsFilter =
        AttributeFilter.newBuilder()
            .setName(EntityConstants.attributeMapPathFor("simpleValue1" + "-" + stringRandomizer1))
            .setOperator(Operator.EXISTS)
            .setAttributeValue(
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("StringValue").build())
                    .build())
            .build();
    entities =
        entityDataServiceClient.query(
            TENANT_ID, Query.newBuilder().setFilter(existsFilter).build());

    assertEquals(1, entities.size());
    assertEquals(createdEntity1, entities.get(0));
  }

  @Test
  public void whenNNewEntitiesAreUpserted_thenExpectNNewEntities() {
    int N = 5;
    Map<String, Entity> externalIdToEntity = new HashMap<>();
    Map<String, AttributeValue> externalIdToAV = new HashMap<>();

    for (int i = 0; i < N; i++) {
      AttributeValue entityId = generateRandomUUIDAttrValue();
      Entity entity =
          Entity.newBuilder()
              .setTenantId(TENANT_ID)
              .setEntityType(EntityType.K8S_POD.name())
              .setEntityName("Some Service")
              .putIdentifyingAttributes(
                  EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID), entityId)
              .build();
      externalIdToEntity.put(entityId.getValue().getString(), entity);
      externalIdToAV.put(entityId.getValue().getString(), entityId);
    }

    entityDataServiceClient.bulkUpsert(TENANT_ID, externalIdToEntity.values());

    // all N entities should have been created
    Map<String, Entity> entityMap = new HashMap<>();
    for (String id : externalIdToEntity.keySet()) {
      List<Entity> readEntity =
          entityDataServiceClient.getEntitiesWithGivenAttribute(
              TENANT_ID,
              EntityType.K8S_POD.name(),
              EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
              externalIdToAV.get(id));
      // exactly one entity exists
      assertEquals(1, readEntity.size());
      assertNotNull(readEntity.get(0).getEntityId());
      entityMap.put(readEntity.get(0).getEntityId(), readEntity.get(0));
    }

    // Try getAndBulkUpsert, verify that the returned entities were in previous state.
    Iterator<Entity> iterator =
        entityDataServiceClient.getAndBulkUpsert(TENANT_ID, externalIdToEntity.values());
    while (iterator.hasNext()) {
      Entity entity = iterator.next();
      assertNotNull(entityMap.get(entity.getEntityId()));
      assertEquals(entityMap.get(entity.getEntityId()), entity);
    }
  }

  @Test
  public void whenNEntitiesAreUpdated_thenExpectThemToBeUpdated() {
    int N = 5;
    Map<String, Entity> externalIdToEntity = new HashMap<>();
    Map<String, Entity> externalIdToNewEntity = new HashMap<>();
    Map<String, AttributeValue> externalIdToAV = new HashMap<>();

    // Create N entities first with an attribute
    for (int i = 0; i < N; i++) {
      AttributeValue entityId = generateRandomUUIDAttrValue();
      Entity entity =
          Entity.newBuilder()
              .setTenantId(TENANT_ID)
              .setEntityType(EntityType.K8S_POD.name())
              .setEntityName("Some Service")
              .putIdentifyingAttributes(
                  EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID), entityId)
              .putAttributes(
                  "foo.key",
                  AttributeValue.newBuilder()
                      .setValue(Value.newBuilder().setString("foo.value.old").build())
                      .build())
              .build();
      externalIdToEntity.put(entityId.getValue().getString(), entity);
      externalIdToAV.put(entityId.getValue().getString(), entityId);
    }

    entityDataServiceClient.bulkUpsert(TENANT_ID, externalIdToEntity.values());

    for (String id : externalIdToEntity.keySet()) {
      List<Entity> readEntity =
          entityDataServiceClient.getEntitiesWithGivenAttribute(
              TENANT_ID,
              EntityType.K8S_POD.name(),
              EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
              externalIdToAV.get(id));
      // exactly one entity exists
      assertEquals(1, readEntity.size());

      System.out.println(
          "Id = "
              + id
              + ", old attribute_val = "
              + readEntity.get(0).getAttributesMap().get("foo.key").getValue().getString());
      // check that they contain old value
      Assertions.assertEquals(
          "foo.value.old",
          readEntity.get(0).getAttributesMap().get("foo.key").getValue().getString());
    }

    // Now update the attribute
    for (Map.Entry<String, Entity> entry : externalIdToEntity.entrySet()) {
      Entity entity = entry.getValue();
      String entityId = entry.getKey();
      Entity newEntity =
          Entity.newBuilder()
              .setEntityType(entity.getEntityType())
              .setEntityName(entity.getEntityName())
              .putIdentifyingAttributes(
                  EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                  entity
                      .getIdentifyingAttributesMap()
                      .get(EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID)))
              .putAttributes(
                  "foo.key",
                  AttributeValue.newBuilder()
                      .setValue(Value.newBuilder().setString("foo.value.new").build())
                      .build())
              .build();
      externalIdToNewEntity.put(entityId, newEntity);
    }

    // upsert
    entityDataServiceClient.bulkUpsert(TENANT_ID, externalIdToNewEntity.values());

    // verify if the entities have been updated
    for (String id : externalIdToEntity.keySet()) {
      List<Entity> readEntity =
          entityDataServiceClient.getEntitiesWithGivenAttribute(
              TENANT_ID,
              EntityType.K8S_POD.name(),
              EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
              externalIdToAV.get(id));
      // exactly one entity exists
      assertEquals(1, readEntity.size());

      System.out.println(
          "Id = "
              + id
              + ", new attribute_val = "
              + readEntity.get(0).getAttributesMap().get("foo.key").getValue().getString());
      // check that they contain old value
      Assertions.assertEquals(
          "foo.value.new",
          readEntity.get(0).getAttributesMap().get("foo.key").getValue().getString());
    }
  }

  @Test
  public void testUpsertAndGetEnrichedEntity() {
    EnrichedEntity entity =
        EnrichedEntity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service")
            .setEntityId(UUID.randomUUID().toString())
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .build();
    upsertAndVerify(entity);
  }

  @Test
  public void testBulkUpsertEnrichedEntities() {
    EnrichedEntity entity1 =
        EnrichedEntity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service")
            .setEntityId(UUID.randomUUID().toString())
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .build();
    EnrichedEntity entity2 =
        EnrichedEntity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.DOCKER_CONTAINER.name())
            .setEntityName("Some container")
            .setEntityId(UUID.randomUUID().toString())
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .putRelatedEntities(
                EntityType.K8S_POD.name(),
                EnrichedEntities.newBuilder().addEntities(entity1).build())
            .build();

    entityDataServiceClient.upsertEnrichedEntities(
        TENANT_ID, EnrichedEntities.newBuilder().addEntities(entity1).addEntities(entity2).build());

    EnrichedEntity actualEntity1 =
        entityDataServiceClient.getEnrichedEntityById(TENANT_ID, entity1.getEntityId());
    assertEquals(entity1, actualEntity1);

    EnrichedEntity actualEntity2 =
        entityDataServiceClient.getEnrichedEntityById(TENANT_ID, entity2.getEntityId());
    assertEquals(entity2, actualEntity2);
  }

  @Test
  public void testEntityQueryOrderBy() {
    Entity entity1 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service 1")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .putAttributes(
                "foo",
                AttributeValue.newBuilder().setValue(Value.newBuilder().setInt(5).build()).build())
            .build();
    Entity createdEntity1 = entityDataServiceClient.upsert(entity1);
    assertNotNull(createdEntity1);
    assertNotNull(createdEntity1.getEntityId().trim());

    Entity entity2 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("Some Service 2")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .putAttributes(
                "foo",
                AttributeValue.newBuilder().setValue(Value.newBuilder().setInt(10).build()).build())
            .build();
    Entity createdEntity2 = entityDataServiceClient.upsert(entity2);
    assertNotNull(createdEntity2);
    assertNotNull(createdEntity2.getEntityId().trim());

    // Query by field name
    Query entityNameQuery =
        Query.newBuilder()
            .setEntityType(EntityType.K8S_POD.name())
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setOrder(SortOrder.DESC)
                    .setName("entityName")
                    .build())
            .build();
    List<Entity> entitiesList = entityDataServiceClient.query(TENANT_ID, entityNameQuery);
    assertTrue(entitiesList.size() > 1);
    assertTrue(entitiesList.contains(createdEntity1) && entitiesList.contains(createdEntity2));
    // ordered such that entity with "larger" entity name value is listed earlier
    assertTrue(entitiesList.indexOf(createdEntity2) < entitiesList.indexOf(createdEntity1));

    // Query by attribute
    Query attributeQuery =
        Query.newBuilder()
            .setEntityType(EntityType.K8S_POD.name())
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setOrder(SortOrder.DESC)
                    .setName("attributes.foo")
                    .build())
            .build();
    entitiesList = entityDataServiceClient.query(TENANT_ID, attributeQuery);
    assertTrue(entitiesList.size() > 1);
    assertTrue(entitiesList.contains(createdEntity1) && entitiesList.contains(createdEntity2));
    // ordered such that entity with "larger" attributes value is listed earlier
    assertTrue(entitiesList.indexOf(createdEntity2) < entitiesList.indexOf(createdEntity1));
  }

  @Test
  public void testDifferentFilterForMatchingLabelsQuery() {
    Entity entity = createEntityWithAttributeLabels("Some Service 1", List.of("v1", "v2", "v3"));
    Entity createdEntity1 = entityDataServiceClient.upsert(entity);
    assertNotNull(createdEntity1);
    assertNotNull(createdEntity1.getEntityId().trim());

    entity = createEntityWithAttributeLabels("Some Service 2", List.of("v01", "v02", "v03"));
    Entity createdEntity2 = entityDataServiceClient.upsert(entity);
    assertNotNull(createdEntity2);
    assertNotNull(createdEntity2.getEntityId().trim());

    entity = createEntityWithAttributeLabels("Some Service 3", List.of("v01", "v2", "v4"));
    Entity createdEntity3 = entityDataServiceClient.upsert(entity);
    assertNotNull(createdEntity3);
    assertNotNull(createdEntity3.getEntityId().trim());

    entity = createEntityWithAttributeLabels("Some Service 4", List.of("b1", "b2", "b3"));
    Entity createdEntity4 = entityDataServiceClient.upsert(entity);
    assertNotNull(createdEntity4);
    assertNotNull(createdEntity4.getEntityId().trim());

    entity = createEntityWithOutAttributeLabels("Some Service 5", "foo", "bar");
    Entity createdEntity5 = entityDataServiceClient.upsert(entity);
    assertNotNull(createdEntity5);
    assertNotNull(createdEntity5.getEntityId().trim());

    // test NOT_IN
    AttributeFilter attributeFilter =
        AttributeFilter.newBuilder()
            .setName("attributes.labels")
            .setOperator(Operator.NOT_IN)
            .setAttributeValue(
                AttributeValue.newBuilder()
                    .setValueList(
                        AttributeValueList.newBuilder()
                            .addValues(
                                AttributeValue.newBuilder()
                                    .setValue(Value.newBuilder().setString("b1")))
                            .addValues(
                                AttributeValue.newBuilder()
                                    .setValue(Value.newBuilder().setString("v4")))))
            .build();

    Query query =
        Query.newBuilder()
            .setEntityType(EntityType.K8S_POD.name())
            .setFilter(attributeFilter)
            .build();
    List<Entity> entitiesList = entityDataServiceClient.query(TENANT_ID, query);
    assertEquals(Set.of(createdEntity1, createdEntity2, createdEntity5), Set.copyOf(entitiesList));

    // test IN
    attributeFilter =
        AttributeFilter.newBuilder()
            .setName("attributes.labels")
            .setOperator(Operator.IN)
            .setAttributeValue(
                AttributeValue.newBuilder()
                    .setValueList(
                        AttributeValueList.newBuilder()
                            .addValues(
                                AttributeValue.newBuilder()
                                    .setValue(Value.newBuilder().setString("b1")))
                            .addValues(
                                AttributeValue.newBuilder()
                                    .setValue(Value.newBuilder().setString("v4")))))
            .build();

    query =
        Query.newBuilder()
            .setEntityType(EntityType.K8S_POD.name())
            .setFilter(attributeFilter)
            .build();
    entitiesList = entityDataServiceClient.query(TENANT_ID, query);
    assertTrue(entitiesList.size() == 2);
    assertTrue(entitiesList.contains(createdEntity3) && entitiesList.contains(createdEntity4));

    // test NEQ
    attributeFilter =
        AttributeFilter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(
                AttributeFilter.newBuilder()
                    .setName("attributes.labels")
                    .setOperator(Operator.NEQ)
                    .setAttributeValue(
                        AttributeValue.newBuilder().setValue(Value.newBuilder().setString("v2"))))
            .addChildFilter(
                AttributeFilter.newBuilder()
                    .setName("attributes.labels")
                    .setOperator(Operator.NEQ)
                    .setAttributeValue(
                        AttributeValue.newBuilder().setValue(Value.newBuilder().setString("b1"))))
            .build();

    query =
        Query.newBuilder()
            .setEntityType(EntityType.K8S_POD.name())
            .setFilter(attributeFilter)
            .build();
    entitiesList = entityDataServiceClient.query(TENANT_ID, query);
    assertTrue(entitiesList.size() == 2);
    assertTrue(entitiesList.contains(createdEntity2) && entitiesList.contains(createdEntity5));

    // test EQ
    attributeFilter =
        AttributeFilter.newBuilder()
            .setName("attributes.labels")
            .setOperator(Operator.EQ)
            .setAttributeValue(
                AttributeValue.newBuilder().setValue(Value.newBuilder().setString("v2")))
            .build();

    query =
        Query.newBuilder()
            .setEntityType(EntityType.K8S_POD.name())
            .setFilter(attributeFilter)
            .build();
    entitiesList = entityDataServiceClient.query(TENANT_ID, query);
    assertTrue(entitiesList.size() == 2);
    assertTrue(entitiesList.contains(createdEntity1) && entitiesList.contains(createdEntity3));
  }

  @Test
  public void testCreateEntityViaUpsertAndMerge() {
    // Scope this test to its own tenant for isolation
    String TENANT_ID = EntityDataServiceTest.TENANT_ID + "_testCreateEntityViaUpsertAndMerge";
    EntityDataServiceBlockingStub entityDataServiceStub = buildStubForTenant(TENANT_ID);
    setupEntityTypes(channel, TENANT_ID);
    // V1 entity
    Entity entityToCreate =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("V1 POD")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .build();
    Entity createdEntity =
        entityDataServiceStub
            .mergeAndUpsertEntity(
                MergeAndUpsertEntityRequest.newBuilder().setEntity(entityToCreate).build())
            .getEntity();

    Entity fetchedEntity =
        entityDataServiceStub.getById(
            ByIdRequest.newBuilder()
                .setEntityId(createdEntity.getEntityId())
                .setEntityType(createdEntity.getEntityType())
                .build());

    assertEquals(createdEntity, fetchedEntity);

    // V2 Entity

    entityToCreate =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityId(UUID.randomUUID().toString())
            .setEntityType(TEST_ENTITY_TYPE_V2)
            .setEntityName("V2 entity")
            .build();
    createdEntity =
        entityDataServiceStub
            .mergeAndUpsertEntity(
                MergeAndUpsertEntityRequest.newBuilder().setEntity(entityToCreate).build())
            .getEntity();

    assertEquals(entityToCreate, createdEntity);

    fetchedEntity =
        entityDataServiceStub.getById(
            ByIdRequest.newBuilder()
                .setEntityId(createdEntity.getEntityId())
                .setEntityType(createdEntity.getEntityType())
                .build());

    assertEquals(createdEntity, fetchedEntity);
  }

  @Test
  public void testUpdateEntityViaUpsertAndMerge() {
    // Scope this test to its own tenant for isolation
    String TENANT_ID = EntityDataServiceTest.TENANT_ID + "_testUpdateEntityViaUpsertAndMerge";
    EntityDataServiceBlockingStub entityDataServiceStub = buildStubForTenant(TENANT_ID);
    setupEntityTypes(channel, TENANT_ID);

    Map<String, AttributeValue> createAttributes = Map.of("some-attr", stringValue("v1"));
    Map<String, AttributeValue> updateAttributes = Map.of("some-other-attr", stringValue("v2"));

    // V1 entity
    Entity entityToCreate =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("V1 UPDATE POD")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .putAllAttributes(createAttributes)
            .build();

    entityDataServiceStub.mergeAndUpsertEntity(
        MergeAndUpsertEntityRequest.newBuilder().setEntity(entityToCreate).build());

    Entity entityUpdate =
        entityToCreate.toBuilder().clearAttributes().putAllAttributes(updateAttributes).build();
    Entity updatedEntity =
        entityDataServiceStub
            .mergeAndUpsertEntity(
                MergeAndUpsertEntityRequest.newBuilder().setEntity(entityUpdate).build())
            .getEntity();

    Map<String, AttributeValue> combinedAttributes =
        ImmutableMap.<String, AttributeValue>builder()
            .putAll(createAttributes)
            .putAll(entityToCreate.getIdentifyingAttributesMap())
            .putAll(updateAttributes)
            .build();

    assertEquals(combinedAttributes, updatedEntity.getAttributesMap());

    // V2 Entity

    entityToCreate =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityId(UUID.randomUUID().toString())
            .setEntityType(TEST_ENTITY_TYPE_V2)
            .setEntityName("V2 entity update")
            .putAllAttributes(createAttributes)
            .build();
    entityDataServiceStub.mergeAndUpsertEntity(
        MergeAndUpsertEntityRequest.newBuilder().setEntity(entityToCreate).build());
    entityUpdate =
        entityToCreate.toBuilder().clearAttributes().putAllAttributes(updateAttributes).build();

    updatedEntity =
        entityDataServiceStub
            .mergeAndUpsertEntity(
                MergeAndUpsertEntityRequest.newBuilder().setEntity(entityUpdate).build())
            .getEntity();

    combinedAttributes =
        ImmutableMap.<String, AttributeValue>builder()
            .putAll(createAttributes)
            .putAll(updateAttributes)
            .build();
    assertEquals(
        entityToCreate.toBuilder().clearAttributes().putAllAttributes(combinedAttributes).build(),
        updatedEntity);
  }

  @Test
  public void testUpdateEntityWithPredicateViaUpsertAndMerge() {
    // Scope this test to its own tenant for isolation
    String TENANT_ID =
        EntityDataServiceTest.TENANT_ID + "testUpdateEntityWithPredicateViaUpsertAndMerge";
    EntityDataServiceBlockingStub entityDataServiceStub = buildStubForTenant(TENANT_ID);
    setupEntityTypes(channel, TENANT_ID);

    Map<String, AttributeValue> createAttributes = Map.of("some-attr", stringValue("v1"));
    Map<String, AttributeValue> updateAttributes = Map.of("some-attr", stringValue("v2"));
    // Expect create and the first update to succeed, the second to fail
    UpsertCondition condition =
        UpsertCondition.newBuilder()
            .setPropertyPredicate(
                Predicate.newBuilder()
                    .setAttributeKey("some-attr")
                    .setOperator(PredicateOperator.PREDICATE_OPERATOR_EQUALS)
                    .setValue(stringValue("v1")))
            .build();

    // V1 entity
    Entity entityToCreate =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName("V1 UPDATE PREDICATE POD")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .putAllAttributes(createAttributes)
            .build();
    // Successful create
    Entity createdEntity =
        entityDataServiceStub
            .mergeAndUpsertEntity(
                MergeAndUpsertEntityRequest.newBuilder()
                    .setEntity(entityToCreate)
                    .setUpsertCondition(condition)
                    .build())
            .getEntity();

    assertEquals(stringValue("v1"), createdEntity.getAttributesMap().get("some-attr"));

    Entity entityUpdate = entityToCreate.toBuilder().putAllAttributes(updateAttributes).build();
    Entity updatedEntity =
        entityDataServiceStub
            .mergeAndUpsertEntity(
                MergeAndUpsertEntityRequest.newBuilder()
                    .setEntity(entityUpdate)
                    .setUpsertCondition(condition)
                    .build())
            .getEntity();
    // Successful update
    assertEquals(stringValue("v2"), updatedEntity.getAttributesMap().get("some-attr"));

    Entity secondEntityUpdate =
        entityToCreate.toBuilder().putAttributes("some-new-attr", stringValue("foo")).build();
    Entity secondUpdatedEntity =
        entityDataServiceStub
            .mergeAndUpsertEntity(
                MergeAndUpsertEntityRequest.newBuilder()
                    .setEntity(secondEntityUpdate)
                    .setUpsertCondition(condition)
                    .build())
            .getEntity();
    // No change, update rejected
    assertFalse(secondUpdatedEntity.getAttributesMap().containsKey("some-new-attr"));
    assertEquals(stringValue("v2"), secondUpdatedEntity.getAttributesMap().get("some-attr"));

    // V2 Entity
    entityToCreate =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityId(UUID.randomUUID().toString())
            .setEntityType(TEST_ENTITY_TYPE_V2)
            .setEntityName("V2 entity predicate update")
            .putAllAttributes(createAttributes)
            .build();

    // Successful create
    createdEntity =
        entityDataServiceStub
            .mergeAndUpsertEntity(
                MergeAndUpsertEntityRequest.newBuilder()
                    .setEntity(entityToCreate)
                    .setUpsertCondition(condition)
                    .build())
            .getEntity();

    assertEquals(stringValue("v1"), createdEntity.getAttributesMap().get("some-attr"));

    entityUpdate = entityToCreate.toBuilder().putAllAttributes(updateAttributes).build();
    updatedEntity =
        entityDataServiceStub
            .mergeAndUpsertEntity(
                MergeAndUpsertEntityRequest.newBuilder()
                    .setEntity(entityUpdate)
                    .setUpsertCondition(condition)
                    .build())
            .getEntity();
    // Successful update
    assertEquals(stringValue("v2"), updatedEntity.getAttributesMap().get("some-attr"));

    secondEntityUpdate =
        entityToCreate.toBuilder().putAttributes("some-new-attr", stringValue("foo")).build();
    secondUpdatedEntity =
        entityDataServiceStub
            .mergeAndUpsertEntity(
                MergeAndUpsertEntityRequest.newBuilder()
                    .setEntity(secondEntityUpdate)
                    .setUpsertCondition(condition)
                    .build())
            .getEntity();
    // No change, update rejected
    assertFalse(secondUpdatedEntity.getAttributesMap().containsKey("some-new-attr"));
    assertEquals(stringValue("v2"), secondUpdatedEntity.getAttributesMap().get("some-attr"));
  }

  private Entity createEntityWithAttributeLabels(String entityName, List<String> labels) {
    AttributeValueList.Builder listBuilder = AttributeValueList.newBuilder();
    labels.forEach(
        label ->
            listBuilder.addValues(
                AttributeValue.newBuilder().setValue(Value.newBuilder().setString(label))));
    AttributeValue value = AttributeValue.newBuilder().setValueList(listBuilder.build()).build();

    Entity entity =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName(entityName)
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .putAttributes("labels", value)
            .build();

    return entity;
  }

  private Entity createEntityWithOutAttributeLabels(
      String entityName, String attrName, String attrValue) {
    Entity entity =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.K8S_POD.name())
            .setEntityName(entityName)
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_EXTERNAL_ID),
                generateRandomUUIDAttrValue())
            .putAttributes(
                attrName,
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString(attrValue).build())
                    .build())
            .build();
    return entity;
  }

  private AttributeValue generateRandomUUIDAttrValue() {
    return AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setString(UUID.randomUUID().toString()).build())
        .build();
  }

  private void upsertAndVerify(Entity entityToCreate) {
    Entity createdEntity = entityDataServiceClient.upsert(entityToCreate);
    assertNotNull(entityToCreate);
    assertNotNull(createdEntity.getEntityId().trim());

    Entity actualBackendEntity =
        entityDataServiceClient.getById(TENANT_ID, createdEntity.getEntityId());

    assertEquals(createdEntity, actualBackendEntity);
  }

  private void upsertAndVerify(EnrichedEntity enrichedEntity) {
    EnrichedEntity createdEntity = entityDataServiceClient.upsertEnrichedEntity(enrichedEntity);
    assertNotNull(enrichedEntity);
    assertNotNull(createdEntity.getEntityId().trim());

    EnrichedEntity actualBackendEntity =
        entityDataServiceClient.getEnrichedEntityById(TENANT_ID, createdEntity.getEntityId());
    assertEquals(createdEntity, actualBackendEntity);
  }

  private EntityDataServiceBlockingStub buildStubForTenant(String tenant) {
    return EntityDataServiceGrpc.newBlockingStub(channel)
        .withCallCredentials(
            new RequestContextAsCreds() {
              @Override
              public void applyRequestMetadata(
                  RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
                // Make it a bit easier to use in tests to build the into the stub
                this.applyRequestContext(applier, RequestContext.forTenantId(tenant));
              }
            });
  }

  private AttributeValue stringValue(String value) {
    return AttributeValue.newBuilder().setValue(Value.newBuilder().setString(value)).build();
  }
}
