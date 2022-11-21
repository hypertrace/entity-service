package org.hypertrace.entity.change.event.v1;

import org.apache.kafka.common.serialization.Serializer;

public class EntityChangeEventKeySerializer implements Serializer<EntityChangeEventKey> {

  @Override
  public byte[] serialize(String topic, EntityChangeEventKey data) {
    return data.toByteArray();
  }
}
