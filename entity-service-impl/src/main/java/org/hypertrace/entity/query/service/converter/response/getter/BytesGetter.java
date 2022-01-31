package org.hypertrace.entity.query.service.converter.response.getter;

import static org.hypertrace.entity.query.service.v1.ValueType.BYTES;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Singleton;
import com.google.protobuf.ByteString;
import java.io.IOException;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.Value;

@Singleton
public class BytesGetter implements ValueGetter {

  @Override
  public boolean matches(final JsonNode jsonNode) {
    return jsonNode != null && jsonNode.isBinary();
  }

  @Override
  public Value getValue(final JsonNode jsonNode) throws ConversionException {
    final byte[] binaryValue;

    try {
      binaryValue = jsonNode.binaryValue();
    } catch (final IOException e) {
      throw new ConversionException("Unable to convert to bytes", e);
    }

    final ByteString byteString = ByteString.copyFrom(binaryValue);
    return Value.newBuilder().setValueType(BYTES).setBytes(byteString).build();
  }
}
