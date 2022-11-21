package org.hypertrace.entity.change.event.v1;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;

public class EntityChangeEventValueDeserializer implements Deserializer<EntityChangeEventValue> {

  @Override
  public EntityChangeEventValue deserialize(String topic, byte[] data) {
    try {
      return EntityChangeEventValue.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
