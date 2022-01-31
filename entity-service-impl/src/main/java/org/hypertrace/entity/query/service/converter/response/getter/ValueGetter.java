package org.hypertrace.entity.query.service.converter.response.getter;

import com.fasterxml.jackson.databind.JsonNode;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.Value;

public interface ValueGetter {
  boolean matches(final JsonNode jsonNode);

  Value getValue(final JsonNode jsonNode) throws ConversionException;
}
