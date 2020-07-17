package org.hypertrace.entity.data.service.client;

import java.util.HashMap;
import java.util.Map;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.ByTypeAndIdentifyingAttributes;
import org.hypertrace.entity.data.service.v1.Value;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@link EdsTypeAndIdAttributesCacheKey}
 * */
public class EdsTypeAndIdAttributesCacheKeyTest {

  @Test
  public void testEqualsHashcode() {
    Map<String, AttributeValue> identifyingAttributesMap1 = new HashMap<>();
    identifyingAttributesMap1.put("entity_name", AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setString("GET /products").build()).build());
    identifyingAttributesMap1.put("is_active", AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setBoolean(true).build()).build());

    ByTypeAndIdentifyingAttributes attributes1 = ByTypeAndIdentifyingAttributes.newBuilder()
        .setEntityType("API")
        .putAllIdentifyingAttributes(identifyingAttributesMap1)
        .build();

    // cacheKey1, cacheKey2 using same input
    EdsTypeAndIdAttributesCacheKey cacheKey1 = new EdsTypeAndIdAttributesCacheKey("tenant1",
        attributes1);
    EdsTypeAndIdAttributesCacheKey cacheKey2 = new EdsTypeAndIdAttributesCacheKey("tenant1",
        attributes1);

    Assertions.assertEquals(cacheKey1, cacheKey2);
    Assertions.assertEquals(cacheKey1.hashCode(), cacheKey2.hashCode());

    // different tenant value
    EdsTypeAndIdAttributesCacheKey cacheKey3 = new EdsTypeAndIdAttributesCacheKey("tenant2",
        attributes1);
    Assertions.assertNotEquals(cacheKey1, cacheKey3);

    // only entity_type change, and same attributes
    ByTypeAndIdentifyingAttributes attributes2 = ByTypeAndIdentifyingAttributes.newBuilder()
        .setEntityType("SERVICE")
        .putAllIdentifyingAttributes(identifyingAttributesMap1)
        .build();
    EdsTypeAndIdAttributesCacheKey cacheKey4 = new EdsTypeAndIdAttributesCacheKey("tenant1",
        attributes2);
    Assertions.assertNotEquals(cacheKey1, cacheKey4);

    // entity_type is same, but attributes are different
    Map<String, AttributeValue> identifyingAttributesMap2 = new HashMap<>();
    identifyingAttributesMap1.put("entity_name", AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setString("GET /books").build()).build());
    identifyingAttributesMap1.put("is_active", AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setBoolean(false).build()).build());

    ByTypeAndIdentifyingAttributes attributes3 = ByTypeAndIdentifyingAttributes.newBuilder()
        .setEntityType("API")
        .putAllIdentifyingAttributes(identifyingAttributesMap2)
        .build();
    EdsTypeAndIdAttributesCacheKey cacheKey5 = new EdsTypeAndIdAttributesCacheKey("tenant1",
        attributes3);
    Assertions.assertNotEquals(cacheKey1, cacheKey5);
  }

}
