package org.hypertrace.entity.service.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannelBuilder;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.hypertrace.core.serviceframework.IntegrationTestServerUtil;
import org.hypertrace.entity.constants.v1.CommonAttribute;
import org.hypertrace.entity.data.service.client.EntityDataServiceClient;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.service.client.config.EntityServiceClientConfig;
import org.hypertrace.entity.service.client.config.EntityServiceTestConfig;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.type.service.v1.AttributeKind;
import org.hypertrace.entity.type.service.v1.AttributeType;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link org.hypertrace.entity.query.service.client.EntityQueryServiceClient}
 */
public class EntityQueryServiceTest {

  private static EntityQueryServiceClient entityQueryServiceClient;
  private static EntityDataServiceClient entityDataServiceClient;
  private static final String TENANT_ID =
      "__testTenant__" + EntityQueryServiceClient.class.getSimpleName();

  @BeforeAll
  public static void setUp() {
    IntegrationTestServerUtil.startServices(new String[]{"entity-service"});
    EntityServiceClientConfig esConfig = EntityServiceTestConfig.getClientConfig();
    Channel channel = ClientInterceptors.intercept(ManagedChannelBuilder.forAddress(
        esConfig.getHost(), esConfig.getPort()).usePlaintext().build());
    entityQueryServiceClient = new EntityQueryServiceClient(channel);
    entityDataServiceClient = new EntityDataServiceClient(channel);
    setupEntityTypes(channel);
  }

  @AfterAll
  public static void teardown() {
    IntegrationTestServerUtil.shutdownServices();
  }

  private static void setupEntityTypes(Channel channel) {
    org.hypertrace.entity.type.service.client.EntityTypeServiceClient entityTypeServiceV1Client
        = new org.hypertrace.entity.type.service.client.EntityTypeServiceClient(channel);
    entityTypeServiceV1Client.upsertEntityType(
        TENANT_ID,
        org.hypertrace.entity.type.service.v1.EntityType.newBuilder()
            .setName(EntityType.SERVICE.name())
            .addAttributeType(
                AttributeType.newBuilder()
                    .setName(EntityConstants.getValue(CommonAttribute.COMMON_ATTRIBUTE_FQN))
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setIdentifyingAttribute(true)
                    .build())
            .build());
  }

  @Test
  public void testExecute() {
    // create and upsert some entities
    Entity entity1 = Entity.newBuilder()
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

    Entity entity2 = Entity.newBuilder()
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

    Entity entity3 = Entity.newBuilder()
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

    Entity entity4 = Entity.newBuilder()
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

    Entity entity5 = Entity.newBuilder()
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

    EntityQueryRequest queryRequest = EntityQueryRequest.newBuilder()
        .setEntityType(EntityType.SERVICE.name())
        .setFilter(
            Filter.newBuilder()
                .setOperatorValue(Operator.LT.getNumber())
                .setLhs(Expression.newBuilder().setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("SERVICE.createdTime").build()).build())
                .setRhs(Expression.newBuilder().setLiteral(
                    LiteralConstant.newBuilder().setValue(
                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                            .setLong(Instant.now().toEpochMilli())
                            .setValueType(ValueType.LONG)
                            .build())
                        .build()).
                    build())
                .build())
        .addSelection(Expression.newBuilder().setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("SERVICE.id").build()).build())
        .addSelection(Expression.newBuilder().setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("SERVICE.name").build()).build())
        .build();

    // this entity will be filtered out
    Entity entity6 = Entity.newBuilder()
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

    Iterator<ResultSetChunk> resultSetChunkIterator = entityQueryServiceClient.execute(queryRequest, Map.of("x-tenant-id", TENANT_ID));
    List<ResultSetChunk> list = Lists.newArrayList(resultSetChunkIterator);
    assertEquals(3, list.size());
    assertEquals(2, list.get(0).getRowCount());
    assertEquals(1, list.get(0).getChunkId());
    assertFalse(list.get(0).getIsLastChunk());
    assertEquals(2, list.get(1).getRowCount());
    assertEquals(2, list.get(1).getChunkId());
    assertFalse(list.get(1).getIsLastChunk());
    assertEquals(1, list.get(2).getRowCount());
    assertEquals(3, list.get(2).getChunkId());
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
    EntityQueryRequest queryRequestNoResult = EntityQueryRequest.newBuilder()
        .setEntityType(EntityType.SERVICE.name())
        .setFilter(
            Filter.newBuilder()
                .setOperatorValue(Operator.GT.getNumber())
                .setLhs(Expression.newBuilder().setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("SERVICE.createdTime").build()).build())
                .setRhs(Expression.newBuilder().setLiteral(
                    LiteralConstant.newBuilder().setValue(
                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                            .setLong(Instant.now().toEpochMilli())
                            .setValueType(ValueType.LONG)
                            .build())
                        .build()).
                    build())
                .build())
        .addSelection(Expression.newBuilder().setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("SERVICE.id").build()).build())
        .addSelection(Expression.newBuilder().setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("SERVICE.name").build()).build())
        .build();

    Iterator<ResultSetChunk> resultSetChunkIterator = entityQueryServiceClient.execute(queryRequestNoResult, Map.of("x-tenant-id", TENANT_ID));
    List<ResultSetChunk> list = Lists.newArrayList(resultSetChunkIterator);

    assertEquals(1, list.size());
    assertEquals(1, list.get(0).getChunkId());
    assertTrue(list.get(0).getIsLastChunk());
    assertTrue(list.get(0).getResultSetMetadata().getColumnMetadataCount() > 0);
  }

  private AttributeValue generateRandomUUIDAttrValue() {
    return AttributeValue.newBuilder()
        .setValue(Value.newBuilder()
            .setString(UUID.randomUUID().toString())
            .build())
        .build();
  }
}
