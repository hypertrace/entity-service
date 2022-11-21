package org.hypertrace.entity.change.event.v1;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class EntityChangeEventValueSerde implements Serde<EntityChangeEventValue> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public void close() {}

  @Override
  public Serializer<EntityChangeEventValue> serializer() {
    return new EntityChangeEventValueSerializer();
  }

  @Override
  public Deserializer<EntityChangeEventValue> deserializer() {
    return new EntityChangeEventValueDeserializer();
  }
}
