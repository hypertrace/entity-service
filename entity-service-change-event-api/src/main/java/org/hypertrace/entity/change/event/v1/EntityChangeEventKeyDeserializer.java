package org.hypertrace.entity.change.event.v1;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;

public class EntityChangeEventKeyDeserializer implements Deserializer<EntityChangeEventKey> {

  @Override
  public EntityChangeEventKey deserialize(String topic, byte[] data) {
    try {
      return EntityChangeEventKey.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
