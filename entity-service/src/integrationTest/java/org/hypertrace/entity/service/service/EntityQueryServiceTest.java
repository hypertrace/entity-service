package org.hypertrace.entity.service.service;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static org.hypertrace.entity.service.constants.EntityCollectionConstants.ENTITY_TYPES_COLLECTION;
import static org.hypertrace.entity.service.constants.EntityCollectionConstants.RAW_ENTITIES_COLLECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.hypertrace.entity.constants.v1.ApiAttribute;
import org.hypertrace.entity.constants.v1.CommonAttribute;
import org.hypertrace.entity.constants.v1.ServiceAttribute;
import org.hypertrace.entity.data.service.client.EntityDataServiceClient;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.entity.query.service.v1.BulkEntityUpdateRequest;
import org.hypertrace.entity.query.service.v1.BulkEntityUpdateRequest.EntityUpdateInfo;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc.EntityQueryServiceBlockingStub;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.SetAttribute;
import org.hypertrace.entity.query.service.v1.TotalEntitiesRequest;
import org.hypertrace.entity.query.service.v1.TotalEntitiesResponse;
import org.hypertrace.entity.query.service.v1.UpdateOperation;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.service.EntityServiceConfig;
import org.hypertrace.entity.service.client.config.EntityServiceClientConfig;
import org.hypertrace.entity.service.client.config.EntityServiceTestConfig;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.type.service.v1.AttributeKind;
import org.hypertrace.entity.type.service.v1.AttributeType;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Test for {@link org.hypertrace.entity.query.service.client.EntityQueryServiceClient} */
public class EntityQueryServiceTest {

  private static final Logger LOG = LoggerFactory.getLogger(EntityQueryServiceTest.class);
  private static final Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(LOG);

  private static EntityQueryServiceBlockingStub entityQueryServiceClient;
  // needed to create entities
  private static EntityDataServiceClient entityDataServiceClient;

  private static ManagedChannel channel;
  private static Datastore datastore;

  private static final String TENANT_ID =
      "__testTenant__" + EntityQueryServiceClient.class.getSimpleName();
  private static final String API_NAME = "GET /products";
  private static final String API_TYPE = "HTTP";
  private static final String SERVICE_ID = generateRandomUUID();

  private static Map<String, String> apiAttributesMap;
  private static Map<String, String> HEADERS = Map.of("x-tenant-id", TENANT_ID);
  // attributes defined in application.conf in attribute map
  private static final String API_DISCOVERY_STATE_ATTR = "API.apiDiscoveryState";
  private static final String API_HTTP_METHOD_ATTR = "API.httpMethod";
  private static final int CONTAINER_STARTUP_ATTEMPTS = 5;
  private static GenericContainer<?> mongo;

  @BeforeAll
  public static void setUp() throws Exception {
    mongo =
        new GenericContainer<>(DockerImageName.parse("mongo:4.4.0"))
            .withExposedPorts(27017)
            .withStartupAttempts(CONTAINER_STARTUP_ATTEMPTS)
            .waitingFor(Wait.forListeningPort())
            .withLogConsumer(logConsumer);
    mongo.start();

    withEnvironmentVariable("MONGO_HOST", mongo.getHost())
        .and("MONGO_PORT", mongo.getMappedPort(27017).toString())
        .execute(
            () -> {
              ConfigFactory.invalidateCaches();
              IntegrationTestServerUtil.startServices(new String[] {"entity-service"});
            });

    EntityServiceClientConfig entityServiceTestConfig = EntityServiceTestConfig.getClientConfig();
    channel =
        ManagedChannelBuilder.forAddress(
                entityServiceTestConfig.getHost(), entityServiceTestConfig.getPort())
            .usePlaintext()
            .build();
    entityQueryServiceClient =
        EntityQueryServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    entityDataServiceClient = new EntityDataServiceClient(channel);

    datastore = getDatastore();

    Map<String, Map<String, String>> attributesMap = getAttributesMap();
    apiAttributesMap = attributesMap.get(EntityType.API.name());
  }

  @BeforeEach
  public void clearCollections() {
    clearCollection(ENTITY_TYPES_COLLECTION);
    clearCollection(RAW_ENTITIES_COLLECTION);
    setupEntityTypes(channel);
  }

  private static void clearCollection(String collName) {
    datastore.deleteCollection(collName);
    datastore.createCollection(collName, null);
  }

  @AfterAll
  public static void teardown() {
    channel.shutdown();
    IntegrationTestServerUtil.shutdownServices();
    mongo.stop();
  }

  private static void setupEntityTypes(Channel channel) {
    org.hypertrace.entity.type.service.client.EntityTypeServiceClient entityTypeServiceV1Client =
        new org.hypertrace.entity.type.service.client.EntityTypeServiceClient(channel);
    entityTypeServiceV1Client.upsertEntityType(
        TENANT_ID,
        org.hypertrace.entity.type.service.v1.EntityType.newBuilder()
            .setName(EntityType.SERVICE.name())
            .addAttributeType(
                AttributeType.newBuilder()
                    .setName(EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN))
                    .setIdentifyingAttribute(true))
            .build());
    entityTypeServiceV1Client.upsertEntityType(
        TENANT_ID,
        org.hypertrace.entity.type.service.v1.EntityType.newBuilder()
            .setName(EntityType.API.name())
            .addAttributeType(
                AttributeType.newBuilder()
                    .setName(EntityConstants.getValue(ServiceAttribute.SERVICE_ATTRIBUTE_ID))
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setIdentifyingAttribute(true)
                    .build())
            .addAttributeType(
                AttributeType.newBuilder()
                    .setName(EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_NAME))
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setIdentifyingAttribute(true)
                    .build())
            .addAttributeType(
                AttributeType.newBuilder()
                    .setName(EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_API_TYPE))
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setIdentifyingAttribute(true)
                    .build())
            .build());
  }

  @Test
  public void testExecute() {
    // create and upsert some entities
    Entity entity1 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.SERVICE.name())
            .setEntityName("Some Service 1")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN),
                generateRandomUUIDAttrValue())
            .build();
    Entity createdEntity1 = entityDataServiceClient.upsert(entity1);
    assertNotNull(createdEntity1);
    assertFalse(createdEntity1.getEntityId().trim().isEmpty());

    Entity entity2 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.SERVICE.name())
            .setEntityName("Some Service 2")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN),
                generateRandomUUIDAttrValue())
            .build();
    Entity createdEntity2 = entityDataServiceClient.upsert(entity2);
    assertNotNull(createdEntity2);
    assertFalse(createdEntity2.getEntityId().trim().isEmpty());

    Entity entity3 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.SERVICE.name())
            .setEntityName("Some Service 3")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN),
                generateRandomUUIDAttrValue())
            .build();
    Entity createdEntity3 = entityDataServiceClient.upsert(entity3);
    assertNotNull(createdEntity3);
    assertFalse(createdEntity3.getEntityId().trim().isEmpty());

    Entity entity4 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.SERVICE.name())
            .setEntityName("Some Service 4")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN),
                generateRandomUUIDAttrValue())
            .build();
    Entity createdEntity4 = entityDataServiceClient.upsert(entity4);
    assertNotNull(createdEntity4);
    assertFalse(createdEntity4.getEntityId().trim().isEmpty());

    Entity entity5 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.SERVICE.name())
            .setEntityName("Some Service 5")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN),
                generateRandomUUIDAttrValue())
            .build();
    Entity createdEntity5 = entityDataServiceClient.upsert(entity5);
    assertNotNull(createdEntity5);
    assertFalse(createdEntity5.getEntityId().trim().isEmpty());

    EntityQueryRequest queryRequest =
        EntityQueryRequest.newBuilder()
            .setEntityType(EntityType.SERVICE.name())
            .setFilter(
                Filter.newBuilder()
                    .setOperatorValue(Operator.LT.getNumber())
                    .setLhs(
                        Expression.newBuilder()
                            .setColumnIdentifier(
                                ColumnIdentifier.newBuilder()
                                    .setColumnName("SERVICE.createdTime")
                                    .build())
                            .build())
                    .setRhs(
                        Expression.newBuilder()
                            .setLiteral(
                                LiteralConstant.newBuilder()
                                    .setValue(
                                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                                            .setLong(Instant.now().toEpochMilli())
                                            .setValueType(ValueType.LONG)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("SERVICE.id").build())
                    .build())
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("SERVICE.name").build())
                    .build())
            .build();

    // this entity will be filtered out
    Entity entity6 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.SERVICE.name())
            .setEntityName("Some Service 6")
            .putIdentifyingAttributes(
                EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN),
                generateRandomUUIDAttrValue())
            .build();
    Entity createdEntity6 = entityDataServiceClient.upsert(entity6);
    assertNotNull(createdEntity6);
    assertFalse(createdEntity6.getEntityId().trim().isEmpty());

    Iterator<ResultSetChunk> resultSetChunkIterator =
        GrpcClientRequestContextUtil.executeWithHeadersContext(
            HEADERS, () -> entityQueryServiceClient.execute(queryRequest));
    List<ResultSetChunk> list = Lists.newArrayList(resultSetChunkIterator);
    assertEquals(3, list.size());
    assertEquals(2, list.get(0).getRowCount());
    assertEquals(0, list.get(0).getChunkId());
    assertFalse(list.get(0).getIsLastChunk());
    assertEquals(2, list.get(1).getRowCount());
    assertEquals(1, list.get(1).getChunkId());
    assertFalse(list.get(1).getIsLastChunk());
    assertEquals(1, list.get(2).getRowCount());
    assertEquals(2, list.get(2).getChunkId());
    assertTrue(list.get(2).getIsLastChunk());

    assertEquals(createdEntity1.getEntityId(), list.get(0).getRow(0).getColumn(0).getString());
    assertEquals(createdEntity1.getEntityName(), list.get(0).getRow(0).getColumn(1).getString());
    assertEquals(createdEntity2.getEntityId(), list.get(0).getRow(1).getColumn(0).getString());
    assertEquals(createdEntity2.getEntityName(), list.get(0).getRow(1).getColumn(1).getString());
    assertEquals(createdEntity3.getEntityId(), list.get(1).getRow(0).getColumn(0).getString());
    assertEquals(createdEntity3.getEntityName(), list.get(1).getRow(0).getColumn(1).getString());
    assertEquals(createdEntity4.getEntityId(), list.get(1).getRow(1).getColumn(0).getString());
    assertEquals(createdEntity4.getEntityName(), list.get(1).getRow(1).getColumn(1).getString());
    assertEquals(createdEntity5.getEntityId(), list.get(2).getRow(0).getColumn(0).getString());
    assertEquals(createdEntity5.getEntityName(), list.get(2).getRow(0).getColumn(1).getString());

    // metadata sent for each chunk
    assertTrue(list.get(0).getResultSetMetadata().getColumnMetadataCount() > 0);
    assertTrue(list.get(1).getResultSetMetadata().getColumnMetadataCount() > 0);
    assertTrue(list.get(2).getResultSetMetadata().getColumnMetadataCount() > 0);
  }

  @Test
  public void testExecute_EmptyResponse() {
    EntityQueryRequest queryRequestNoResult =
        EntityQueryRequest.newBuilder()
            .setEntityType(EntityType.SERVICE.name())
            .setFilter(
                Filter.newBuilder()
                    .setOperatorValue(Operator.GT.getNumber())
                    .setLhs(
                        Expression.newBuilder()
                            .setColumnIdentifier(
                                ColumnIdentifier.newBuilder()
                                    .setColumnName("SERVICE.createdTime")
                                    .build())
                            .build())
                    .setRhs(
                        Expression.newBuilder()
                            .setLiteral(
                                LiteralConstant.newBuilder()
                                    .setValue(
                                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                                            .setLong(Instant.now().toEpochMilli())
                                            .setValueType(ValueType.LONG)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("SERVICE.id").build())
                    .build())
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("SERVICE.name").build())
                    .build())
            .build();

    Iterator<ResultSetChunk> resultSetChunkIterator =
        GrpcClientRequestContextUtil.executeWithHeadersContext(
            HEADERS, () -> entityQueryServiceClient.execute(queryRequestNoResult));
    List<ResultSetChunk> list = Lists.newArrayList(resultSetChunkIterator);

    assertEquals(1, list.size());
    assertEquals(0, list.get(0).getChunkId());
    assertTrue(list.get(0).getIsLastChunk());
    assertTrue(list.get(0).getResultSetMetadata().getColumnMetadataCount() > 0);
  }

  private AttributeValue generateRandomUUIDAttrValue() {
    return AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setString(UUID.randomUUID().toString()).build())
        .build();
  }

  public void testCreateAndGetEntity() {
    // creating an api entity with attributes
    Entity.Builder apiEntityBuilder = createApiEntity(SERVICE_ID, API_NAME, API_TYPE);
    apiEntityBuilder
        .putAttributes(
            apiAttributesMap.get(API_DISCOVERY_STATE_ATTR), createAttribute("DISCOVERED"))
        .putAttributes(apiAttributesMap.get(API_HTTP_METHOD_ATTR), createAttribute("GET"));
    entityDataServiceClient.upsert(apiEntityBuilder.build());

    // querying the api entities
    EntityQueryRequest entityQueryRequest =
        EntityQueryRequest.newBuilder()
            .setEntityType(EntityType.API.name())
            .addSelection(createExpression(API_DISCOVERY_STATE_ATTR))
            .build();
    Iterator<ResultSetChunk> resultSetChunkIterator =
        GrpcClientRequestContextUtil.executeWithHeadersContext(
            HEADERS, () -> entityQueryServiceClient.execute(entityQueryRequest));

    List<String> values = new ArrayList<>();

    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();

      for (Row row : chunk.getRowList()) {
        for (int i = 0; i < row.getColumnCount(); i++) {
          String value = row.getColumnList().get(i).getString();
          values.add(value);
        }
      }
    }

    assertEquals(1, values.size());
    assertEquals("DISCOVERED", values.get(0));
  }

  @Test
  public void testCreateAndGetEntities() {
    // creating an api entity with attributes
    Entity.Builder apiEntityBuilder1 = createApiEntity(SERVICE_ID, "api1", API_TYPE);
    apiEntityBuilder1
        .putAttributes(
            apiAttributesMap.get(API_DISCOVERY_STATE_ATTR), createAttribute("DISCOVERED"))
        .putAttributes(apiAttributesMap.get(API_HTTP_METHOD_ATTR), createAttribute("GET"));
    entityDataServiceClient.upsert(apiEntityBuilder1.build());

    Entity.Builder apiEntityBuilder2 = createApiEntity(SERVICE_ID, "api2", API_TYPE);
    apiEntityBuilder2
        .putAttributes(
            apiAttributesMap.get(API_DISCOVERY_STATE_ATTR), createAttribute("UNDER_DISCOVERY"))
        .putAttributes(apiAttributesMap.get(API_HTTP_METHOD_ATTR), createAttribute("GET"));
    entityDataServiceClient.upsert(apiEntityBuilder2.build());

    // querying the api entity
    EntityQueryRequest entityQueryRequest =
        EntityQueryRequest.newBuilder()
            .setEntityType(EntityType.API.name())
            .addSelection(createExpression(API_DISCOVERY_STATE_ATTR))
            .build();
    Iterator<ResultSetChunk> resultSetChunkIterator =
        GrpcClientRequestContextUtil.executeWithHeadersContext(
            HEADERS, () -> entityQueryServiceClient.execute(entityQueryRequest));

    List<String> values = new ArrayList<>();

    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();

      for (Row row : chunk.getRowList()) {
        for (int i = 0; i < row.getColumnCount(); i++) {
          String value = row.getColumnList().get(i).getString();
          values.add(value);
        }
      }
    }

    assertEquals(2, values.size());
    assertEquals("DISCOVERED", values.get(0));
    assertEquals("UNDER_DISCOVERY", values.get(1));
  }

  @Test
  public void testBulkUpdate() {
    Entity.Builder apiEntityBuilder1 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.API.name())
            .setEntityName("api1")
            .putIdentifyingAttributes(
                EntityConstants.getValue(ServiceAttribute.SERVICE_ATTRIBUTE_ID),
                createAttribute(SERVICE_ID))
            .putIdentifyingAttributes(
                EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_NAME), createAttribute("api1"))
            .putIdentifyingAttributes(
                EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_API_TYPE),
                createAttribute(API_TYPE));
    apiEntityBuilder1
        .putAttributes(
            apiAttributesMap.get(API_DISCOVERY_STATE_ATTR), createAttribute("DISCOVERED"))
        .putAttributes(apiAttributesMap.get(API_HTTP_METHOD_ATTR), createAttribute("GET"));
    Entity entity1 = entityDataServiceClient.upsert(apiEntityBuilder1.build());

    Entity.Builder apiEntityBuilder2 =
        Entity.newBuilder()
            .setTenantId(TENANT_ID)
            .setEntityType(EntityType.API.name())
            .setEntityName("api2")
            .putIdentifyingAttributes(
                EntityConstants.getValue(ServiceAttribute.SERVICE_ATTRIBUTE_ID),
                createAttribute(SERVICE_ID))
            .putIdentifyingAttributes(
                EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_NAME), createAttribute("api2"))
            .putIdentifyingAttributes(
                EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_API_TYPE),
                createAttribute(API_TYPE));
    apiEntityBuilder2
        .putAttributes(
            apiAttributesMap.get(API_DISCOVERY_STATE_ATTR), createAttribute("UNDER_DISCOVERY"))
        .putAttributes(apiAttributesMap.get(API_HTTP_METHOD_ATTR), createAttribute("GET"));
    Entity entity2 = entityDataServiceClient.upsert(apiEntityBuilder2.build());

    // create BulkUpdate request
    UpdateOperation update1 =
        UpdateOperation.newBuilder()
            .setSetAttribute(
                SetAttribute.newBuilder()
                    .setAttribute(
                        ColumnIdentifier.newBuilder().setColumnName(API_DISCOVERY_STATE_ATTR))
                    .setValue(
                        LiteralConstant.newBuilder()
                            .setValue(
                                org.hypertrace.entity.query.service.v1.Value.newBuilder()
                                    .setString("DISCOVERED"))))
            .build();
    UpdateOperation update2 =
        UpdateOperation.newBuilder()
            .setSetAttribute(
                SetAttribute.newBuilder()
                    .setAttribute(ColumnIdentifier.newBuilder().setColumnName(API_HTTP_METHOD_ATTR))
                    .setValue(
                        LiteralConstant.newBuilder()
                            .setValue(
                                org.hypertrace.entity.query.service.v1.Value.newBuilder()
                                    .setString("POST"))))
            .build();
    EntityUpdateInfo updateInfo1 =
        EntityUpdateInfo.newBuilder().addUpdateOperation(update2).build();
    EntityUpdateInfo updateInfo2 =
        EntityUpdateInfo.newBuilder()
            .addUpdateOperation(update1)
            .addUpdateOperation(update2)
            .build();
    BulkEntityUpdateRequest bulkUpdateRequest =
        BulkEntityUpdateRequest.newBuilder()
            .setEntityType(EntityType.API.name())
            .putEntities(entity1.getEntityId(), updateInfo1)
            .putEntities(entity2.getEntityId(), updateInfo2)
            .build();

    Iterator<ResultSetChunk> resultChunkIterator =
        GrpcClientRequestContextUtil.executeWithHeadersContext(
            HEADERS, () -> entityQueryServiceClient.bulkUpdate(bulkUpdateRequest));

    EntityQueryRequest entityQueryRequest =
        EntityQueryRequest.newBuilder()
            .setEntityType(EntityType.API.name())
            .addSelection(createExpression(API_DISCOVERY_STATE_ATTR))
            .addSelection(createExpression(API_HTTP_METHOD_ATTR))
            .build();

    Iterator<ResultSetChunk> resultSetChunkIterator =
        GrpcClientRequestContextUtil.executeWithHeadersContext(
            HEADERS, () -> entityQueryServiceClient.execute(entityQueryRequest));

    List<String> values = new ArrayList<>();

    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();

      for (Row row : chunk.getRowList()) {
        for (int i = 0; i < row.getColumnCount(); i++) {
          String value = row.getColumnList().get(i).getString();
          values.add(value);
        }
      }
    }

    assertEquals(4, values.size());
    assertEquals("DISCOVERED", values.get(0));
    assertEquals("POST", values.get(1));
    assertEquals("DISCOVERED", values.get(2));
    assertEquals("POST", values.get(3));
  }

  @Nested
  class TotalEntities {

    @Test
    public void testTotal() {
      // creating an api entity with attributes
      Entity.Builder apiEntityBuilder1 = createApiEntity(SERVICE_ID, "api1", API_TYPE);
      apiEntityBuilder1
          .putAttributes(
              apiAttributesMap.get(API_DISCOVERY_STATE_ATTR), createAttribute("DISCOVERED"))
          .putAttributes(apiAttributesMap.get(API_HTTP_METHOD_ATTR), createAttribute("GET"));
      entityDataServiceClient.upsert(apiEntityBuilder1.build());

      Entity.Builder apiEntityBuilder2 = createApiEntity(SERVICE_ID, "api2", API_TYPE);
      apiEntityBuilder2
          .putAttributes(
              apiAttributesMap.get(API_DISCOVERY_STATE_ATTR), createAttribute("UNDER_DISCOVERY"))
          .putAttributes(apiAttributesMap.get(API_HTTP_METHOD_ATTR), createAttribute("GET"));
      entityDataServiceClient.upsert(apiEntityBuilder2.build());

      TotalEntitiesRequest totalEntitiesRequest =
          TotalEntitiesRequest.newBuilder().setEntityType(EntityType.API.name()).build();
      TotalEntitiesResponse response =
          GrpcClientRequestContextUtil.executeWithHeadersContext(
              HEADERS, () -> entityQueryServiceClient.total(totalEntitiesRequest));

      assertEquals(2, response.getTotal());
    }

    @Test
    public void testTotalWithFilter() {
      // creating an api entity with attributes
      Entity.Builder apiEntityBuilder1 = createApiEntity(SERVICE_ID, "api1", API_TYPE);
      apiEntityBuilder1
          .putAttributes(
              apiAttributesMap.get(API_DISCOVERY_STATE_ATTR), createAttribute("DISCOVERED"))
          .putAttributes(apiAttributesMap.get(API_HTTP_METHOD_ATTR), createAttribute("GET"));
      entityDataServiceClient.upsert(apiEntityBuilder1.build());

      Entity.Builder apiEntityBuilder2 = createApiEntity(SERVICE_ID, "api2", API_TYPE);
      apiEntityBuilder2
          .putAttributes(
              apiAttributesMap.get(API_DISCOVERY_STATE_ATTR), createAttribute("UNDER_DISCOVERY"))
          .putAttributes(apiAttributesMap.get(API_HTTP_METHOD_ATTR), createAttribute("GET"));
      entityDataServiceClient.upsert(apiEntityBuilder2.build());

      TotalEntitiesRequest totalEntitiesRequest =
          TotalEntitiesRequest.newBuilder()
              .setEntityType(EntityType.API.name())
              .setFilter(
                  Filter.newBuilder()
                      .setOperator(Operator.EQ)
                      .setLhs(
                          Expression.newBuilder()
                              .setColumnIdentifier(
                                  ColumnIdentifier.newBuilder()
                                      .setColumnName(API_DISCOVERY_STATE_ATTR)
                                      .build())
                              .build())
                      .setRhs(
                          Expression.newBuilder()
                              .setLiteral(
                                  LiteralConstant.newBuilder()
                                      .setValue(
                                          org.hypertrace.entity.query.service.v1.Value.newBuilder()
                                              .setString("DISCOVERED")
                                              .setValueType(ValueType.STRING)
                                              .build())
                                      .build())
                              .build())
                      .build())
              .build();
      TotalEntitiesResponse response =
          GrpcClientRequestContextUtil.executeWithHeadersContext(
              HEADERS, () -> entityQueryServiceClient.total(totalEntitiesRequest));

      assertEquals(1, response.getTotal());
    }
  }

  private static String generateRandomUUID() {
    return UUID.randomUUID().toString();
  }

  private AttributeValue createAttribute(String name) {
    return AttributeValue.newBuilder().setValue(Value.newBuilder().setString(name).build()).build();
  }

  private Entity.Builder createApiEntity(String serviceId, String apiName, String apiType) {
    return Entity.newBuilder()
        .setTenantId(TENANT_ID)
        .setEntityType(EntityType.API.name())
        .setEntityName(apiName)
        .putIdentifyingAttributes(
            EntityConstants.getValue(ServiceAttribute.SERVICE_ATTRIBUTE_ID),
            createAttribute(serviceId))
        .putIdentifyingAttributes(
            EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_NAME), createAttribute(apiName))
        .putIdentifyingAttributes(
            EntityConstants.getValue(ApiAttribute.API_ATTRIBUTE_API_TYPE),
            createAttribute(apiType));
  }

  private Expression createExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName).build())
        .build();
  }

  private static Datastore getDatastore() {
    Config config = ConfigFactory.parseResources("configs/entity-service/application.conf");
    EntityServiceConfig entityServiceConfig =
        new EntityServiceConfig(config.getConfig("entity.service.config"));
    Map<String, String> mongoConfig = new HashMap<>();
    mongoConfig.putIfAbsent("host", mongo.getHost());
    mongoConfig.putIfAbsent("port", mongo.getMappedPort(27017).toString());
    Config dataStoreConfig = ConfigFactory.parseMap(mongoConfig);
    String dataStoreType = entityServiceConfig.getDataStoreType();
    return DatastoreProvider.getDatastore(dataStoreType, dataStoreConfig);
  }

  private static Map<String, Map<String, String>> getAttributesMap() {
    String attributesPrefix = "attributes.";
    Config config = ConfigFactory.parseResources("configs/entity-service/application.conf");
    List<? extends Config> attributeList = config.getConfigList("entity.service.attributeMap");
    Map<String, Map<String, String>> attributesMap = new HashMap<>();
    attributeList.forEach(
        attribute -> {
          String scope = attribute.getString("scope");
          if (!attributesMap.containsKey(scope)) {
            attributesMap.put(scope, new HashMap<>());
          }
          Map<String, String> scopedAttributesMap = attributesMap.get(scope);
          String attributeName = attribute.getString("name");
          String subDocPath = attribute.getString("subDocPath");
          if (subDocPath.startsWith(attributesPrefix)) {
            subDocPath = subDocPath.substring(attributesPrefix.length());
          }
          if (!scopedAttributesMap.containsKey(attributeName)) {
            scopedAttributesMap.put(attributeName, subDocPath);
          }
        });

    return attributesMap;
  }
}
