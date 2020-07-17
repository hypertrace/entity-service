package org.hypertrace.entity.data.service.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.ByTypeAndIdentifyingAttributes;
import org.hypertrace.entity.data.service.v1.EnrichedEntity;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.client.config.EntityServiceClientCacheConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EdsCacheClientTest {

  EdsCacheClient edsCacheClient;
  EntityDataServiceClient entityDataServiceClient;

  @BeforeEach
  public void setUp() {
    entityDataServiceClient = mock(EntityDataServiceClient.class);
    edsCacheClient = new EdsCacheClient(entityDataServiceClient,
        EntityServiceClientCacheConfig.DEFAULT);
  }

  @Test
  public void testGetByTypeAndIdentifyingAttributes() {
    String tenantId = "tenant";
    String entityId = "entity-12345";

    Map<String, AttributeValue> identifyingAttributesMap = new HashMap<>();
    identifyingAttributesMap.put("entity_name", AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setString("GET /products").build()).build());
    identifyingAttributesMap.put("is_active", AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setBoolean(true).build()).build());

    Entity entity = Entity.newBuilder()
        .setTenantId(tenantId)
        .setEntityId(entityId)
        .setEntityType("API")
        .setEntityName("GET /products")
        .putAllIdentifyingAttributes(identifyingAttributesMap)
        .build();

    when(entityDataServiceClient.getById(anyString(), anyString())).thenReturn(entity);
    when(entityDataServiceClient.getByTypeAndIdentifyingAttributes(anyString(), any()))
        .thenReturn(entity);

    ByTypeAndIdentifyingAttributes attributes = ByTypeAndIdentifyingAttributes.newBuilder()
        .setEntityType("API")
        .putAllIdentifyingAttributes(identifyingAttributesMap)
        .build();

    edsCacheClient.getByTypeAndIdentifyingAttributes(tenantId, attributes);
    edsCacheClient.getByTypeAndIdentifyingAttributes(tenantId, attributes);

    verify(entityDataServiceClient, times(1)).
        getByTypeAndIdentifyingAttributes("tenant", attributes);
    verify(entityDataServiceClient, never()).
        getById("tenant", "entity-12345");
  }

  @Test
  public void testGetByTypeAndIdentifyingForNull() {
    String tenantId = "tenant";

    Map<String, AttributeValue> identifyingAttributesMap = new HashMap<>();
    identifyingAttributesMap.put("entity_name", AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setString("GET /products").build()).build());
    identifyingAttributesMap.put("is_active", AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setBoolean(true).build()).build());

    when(entityDataServiceClient.getByTypeAndIdentifyingAttributes(anyString(), any()))
        .thenReturn(null);

    ByTypeAndIdentifyingAttributes attributes = ByTypeAndIdentifyingAttributes.newBuilder()
        .setEntityType("API")
        .putAllIdentifyingAttributes(identifyingAttributesMap)
        .build();

    Entity entity = edsCacheClient.getByTypeAndIdentifyingAttributes(tenantId, attributes);
    Assertions.assertNull(entity);

    entity = edsCacheClient.getByTypeAndIdentifyingAttributes(tenantId, attributes);
    Assertions.assertNull(entity);

    verify(entityDataServiceClient, times(2)).
        getByTypeAndIdentifyingAttributes("tenant", attributes);
    verify(entityDataServiceClient, never()).
        getById("tenant", "entity-12345");
  }

  @Test
  public void testGetEnrichedEntityById() {
    String tenantId = "tenant";
    String enrichedEntityId = "enriched-12345";

    Map<String, AttributeValue> identifyingAttributesMap = new HashMap<>();
    identifyingAttributesMap.put("entity_name", AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setString("GET /products").build()).build());
    identifyingAttributesMap.put("is_active", AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setBoolean(true).build()).build());

    EnrichedEntity enrichedEntity = EnrichedEntity.newBuilder()
        .setEntityId(enrichedEntityId)
        .setEntityType("API")
        .setEntityName("GET /products")
        .putAllIdentifyingAttributes(identifyingAttributesMap)
        .build();

    when(entityDataServiceClient.getEnrichedEntityById(anyString(), anyString()))
        .thenReturn(enrichedEntity);

    edsCacheClient.getEnrichedEntityById(tenantId, enrichedEntityId);
    edsCacheClient.getEnrichedEntityById(tenantId, enrichedEntityId);

    verify(entityDataServiceClient, times(1)).
        getEnrichedEntityById("tenant", "enriched-12345");
  }

  @Test
  public void testGetEnrichedEntityByIdForNull() {
    String tenantId = "tenant";
    String enrichedEntityId = "enriched-12345";

    when(entityDataServiceClient.getEnrichedEntityById(anyString(), anyString()))
        .thenReturn(null);

    EnrichedEntity enrichedEntity = edsCacheClient
        .getEnrichedEntityById(tenantId, enrichedEntityId);
    Assertions.assertNull(enrichedEntity);

    enrichedEntity = edsCacheClient.getEnrichedEntityById(tenantId, enrichedEntityId);
    Assertions.assertNull(enrichedEntity);

    verify(entityDataServiceClient, times(2)).
        getEnrichedEntityById("tenant", "enriched-12345");
  }

  @Test
  public void testGetById() {
    String tenantId = "tenant";
    String entityId = "entity-12346";

    Map<String, AttributeValue> identifyingAttributesMap = new HashMap<>();
    identifyingAttributesMap.put("entity_name", AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setString("GET /products").build()).build());
    identifyingAttributesMap.put("is_active", AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setBoolean(true).build()).build());

    Entity entity = Entity.newBuilder()
        .setTenantId(tenantId)
        .setEntityId(entityId)
        .setEntityType("API")
        .setEntityName("GET /products")
        .putAllIdentifyingAttributes(identifyingAttributesMap)
        .build();

    when(entityDataServiceClient.getById(anyString(), anyString())).thenReturn(entity);

    edsCacheClient.getById(tenantId, entityId);
    edsCacheClient.getById(tenantId, entityId);

    verify(entityDataServiceClient, times(1)).
        getById("tenant", "entity-12346");
  }

  @Test
  public void testGetByIdForNull() {
    String tenantId = "tenant";
    String entityId = "entity-12346";

    when(entityDataServiceClient.getById(anyString(), anyString())).thenReturn(null);

    Entity entity = edsCacheClient.getById(tenantId, entityId);
    Assertions.assertNull(entity);

    entity = edsCacheClient.getById(tenantId, entityId);
    Assertions.assertNull(entity);

    verify(entityDataServiceClient, times(2)).
        getById("tenant", "entity-12346");
  }

}
