package org.hypertrace.entity.change.event.v1;

import org.apache.kafka.common.serialization.Serializer;

public class EntityChangeEventValueSerializer implements Serializer<EntityChangeEventValue> {

  @Override
  public byte[] serialize(String topic, EntityChangeEventValue data) {
    return data.toByteArray();
  }
}
