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
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.client.config.EntityServiceClientCacheConfig;
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
  public void testByTypeAndIdentifyingAttributes() {
    String tenantId = "tenant";
    String entityId = "e12345";

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
        getById("tenant", "e12345");
  }
}
