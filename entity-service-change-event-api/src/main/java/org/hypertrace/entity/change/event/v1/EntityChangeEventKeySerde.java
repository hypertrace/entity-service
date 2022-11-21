package org.hypertrace.entity.change.event.v1;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class EntityChangeEventKeySerde implements Serde<EntityChangeEventKey> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public void close() {}

  @Override
  public Serializer<EntityChangeEventKey> serializer() {
    return new EntityChangeEventKeySerializer();
  }

  @Override
  public Deserializer<EntityChangeEventKey> deserializer() {
    return new EntityChangeEventKeyDeserializer();
  }
}
