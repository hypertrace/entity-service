package org.hypertrace.entity.query.service.converter.response;

import com.fasterxml.jackson.databind.JsonNode;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.Row;

public interface JsonNodeConverter {
  Row convertToRow(final JsonNode jsonNode) throws ConversionException;
}
