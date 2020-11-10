package org.hypertrace.entity.data.service;

import java.util.HashMap;
import java.util.Map;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.util.UUIDGenerator;

class EntityIdGenerator {


  String generateEntityId(String tenantId, String entityType,
                                  Map<String, AttributeValue> attributeMap) {
    Map<String, AttributeValue> map = new HashMap<>(attributeMap);
    // Add the tenantId and entityType to the map to make it more unique.
    map.put("customerId",
        AttributeValue.newBuilder().setValue(Value.newBuilder().setString(tenantId)).build());
    map.put("entityType",
        AttributeValue.newBuilder().setValue(Value.newBuilder().setString(entityType)).build());
    return UUIDGenerator.generateUUID(map);
  }

}
