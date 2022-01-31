package org.hypertrace.entity.query.service.converter.response.getter;

import static org.hypertrace.entity.query.service.v1.ValueType.STRING;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Singleton;
import org.hypertrace.entity.query.service.v1.Value;

@Singleton
public class StringGetter implements ValueGetter {
  @Override
  public boolean matches(final JsonNode jsonNode) {
    return jsonNode != null && jsonNode.isTextual();
  }

  @Override
  public Value getValue(final JsonNode jsonNode) {
    return Value.newBuilder().setValueType(STRING).setString(jsonNode.asText()).build();
  }
}
