package org.hypertrace.entity.service.util;

import java.util.HashMap;
import java.util.Map;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.AttributeValueList;
import org.hypertrace.entity.data.service.v1.AttributeValueMap;
import org.hypertrace.entity.data.service.v1.Value;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link UUIDGenerator}
 */
public class UUIDGeneratorTest {

  @Test
  public void testMapWithSimpleValueUUID() {
    Map<String, AttributeValue> map1 = new HashMap<>();
    map1.put("key1",
        AttributeValue.newBuilder().setValue(Value.newBuilder().setString("value1").build())
            .build());
    map1.put("key2",
        AttributeValue.newBuilder().setValue(Value.newBuilder().setString("value2").build())
            .build());
    String uuid1 = UUIDGenerator.generateUUID(map1);

    Map<String, AttributeValue> map2 = new HashMap<>();
    map2.put("key2",
        AttributeValue.newBuilder().setValue(Value.newBuilder().setString("value2").build())
            .build());
    map2.put("key1",
        AttributeValue.newBuilder().setValue(Value.newBuilder().setString("value1").build())
            .build());
    String uuid2 = UUIDGenerator.generateUUID(map2);
    Assertions.assertEquals(uuid1, uuid2);

    Map<String, AttributeValue> map3 = new HashMap<>();
    map3.put("key1",
        AttributeValue.newBuilder().setValue(Value.newBuilder().setString("value1").build())
            .build());
    String uuid3 = UUIDGenerator.generateUUID(map3);
    Assertions.assertNotEquals(uuid1, uuid3);
  }

  @Test
  public void testMapWithListValueUUID() {
    Map<String, AttributeValue> map1 = new HashMap<>();
    map1.put("listKey", AttributeValue.newBuilder()
        .setValueList(AttributeValueList.newBuilder()
            .addValues(
                AttributeValue.newBuilder().setValue(Value.newBuilder().setString("abc").build())
                    .build())
            .addValues(
                AttributeValue.newBuilder().setValue(Value.newBuilder().setString("xyz").build())
                    .build())
            .build())
        .build());
    String uuid1 = UUIDGenerator.generateUUID(map1);

    Map<String, AttributeValue> map2 = new HashMap<>();
    map2.put("listKey", AttributeValue.newBuilder()
        .setValueList(AttributeValueList.newBuilder()
            .addValues(
                AttributeValue.newBuilder().setValue(Value.newBuilder().setString("xyz").build())
                    .build())
            .addValues(
                AttributeValue.newBuilder().setValue(Value.newBuilder().setString("abc").build())
                    .build())
            .build())
        .build());
    String uuid2 = UUIDGenerator.generateUUID(map2);
    Assertions.assertEquals(uuid1, uuid2);

    Map<String, AttributeValue> map3 = new HashMap<>();
    map3.put("listKey", AttributeValue.newBuilder()
        .setValueList(AttributeValueList.newBuilder()
            .addValues(
                AttributeValue.newBuilder().setValue(Value.newBuilder().setString("xyz").build())
                    .build())
            .build())
        .build());
    String uuid3 = UUIDGenerator.generateUUID(map3);
    Assertions.assertNotEquals(uuid1, uuid3);
  }

  @Test
  public void testNestedMapUUID() {
    Map<String, AttributeValue> map1 = new HashMap<>();
    map1.put("nestedKey", AttributeValue.newBuilder()
        .setValueMap(AttributeValueMap.newBuilder()
            .putValues("nestedkey1", AttributeValue.newBuilder()
                .setValue(Value.newBuilder().setString("nestedValue1").build()).build())
            .putValues("nestedkey2", AttributeValue.newBuilder()
                .setValue(Value.newBuilder().setString("nestedValue2").build()).build())
            .putValues("nestedListvalue", AttributeValue.newBuilder()
                .setValueList(AttributeValueList.newBuilder()
                    .addValues(AttributeValue.newBuilder()
                        .setValue(Value.newBuilder()
                            .setLong(1)
                            .build())
                        .build())
                    .addValues(AttributeValue.newBuilder()
                        .setValue(Value.newBuilder()
                            .setLong(2)
                            .build())
                        .build())
                    .build())
                .build())
            .build())
        .build());
    String uuid1 = UUIDGenerator.generateUUID(map1);

    Map<String, AttributeValue> map2 = new HashMap<>();
    map2.put("nestedKey", AttributeValue.newBuilder()
        .setValueMap(AttributeValueMap.newBuilder()
            .putValues("nestedkey2", AttributeValue.newBuilder()
                .setValue(Value.newBuilder().setString("nestedValue2").build()).build())
            .putValues("nestedkey1", AttributeValue.newBuilder()
                .setValue(Value.newBuilder().setString("nestedValue1").build()).build())
            .putValues("nestedListvalue", AttributeValue.newBuilder()
                .setValueList(AttributeValueList.newBuilder()
                    .addValues(AttributeValue.newBuilder()
                        .setValue(Value.newBuilder()
                            .setLong(2)
                            .build())
                        .build())
                    .addValues(AttributeValue.newBuilder()
                        .setValue(Value.newBuilder()
                            .setLong(1)
                            .build())
                        .build())
                    .build())
                .build())
            .build())
        .build());
    String uuid2 = UUIDGenerator.generateUUID(map2);

    Assertions.assertEquals(uuid1, uuid2);
  }

  @Test
  public void testListOfListUUID() {
    Map<String, AttributeValue> map1 = new HashMap<>();
    map1.put("listKey", AttributeValue.newBuilder()
        .setValueList(AttributeValueList.newBuilder()
            .addValues(AttributeValue.newBuilder()
                .setValueList(AttributeValueList.newBuilder()
                    .addValues(AttributeValue.newBuilder()
                        .setValue(Value.newBuilder()
                            .setString("abc")
                            .build())
                        .build())
                    .build())
                .build())
            .build())
        .build());
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      UUIDGenerator.generateUUID(map1);
    });
  }

  @Test
  public void testListOfMapUUID() {
    Map<String, AttributeValue> map1 = new HashMap<>();
    map1.put("listKey", AttributeValue.newBuilder()
        .setValueList(AttributeValueList.newBuilder()
            .addValues(AttributeValue.newBuilder()
                .setValueMap(AttributeValueMap.newBuilder()
                    .putValues("key1", AttributeValue.newBuilder()
                        .setValue(Value.newBuilder()
                            .setString("val1")
                            .build())
                        .build())
                    .build())
                .build())
            .build())
        .build());
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      UUIDGenerator.generateUUID(map1);
    });
  }

  @Test
  public void testVersionStringMatchForUUID() {
    Map<String, AttributeValue> map1 = new HashMap<>();
    map1.put("key1",
        AttributeValue.newBuilder().setValue(Value.newBuilder().setString("value1").build())
            .build());
    map1.put("key2",
        AttributeValue.newBuilder().setValue(Value.newBuilder().setString("value2").build())
            .build());
    String generated = UUIDGenerator.generateUUID(map1);
    Assertions.assertEquals("37d8199a-3e56-30d9-9404-917c157d1c95", generated);
  }
}
