package org.hypertrace.entity.query.service.converter.response;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.Row;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class JsonNodeConverterImpl implements JsonNodeConverter {
  private final ObjectSetter objectSetter;

  @Override
  public Row convertToRow(final JsonNode jsonNode) throws ConversionException {
    Row.Builder rowBuilder = Row.newBuilder();
    objectSetter.setValue(jsonNode, rowBuilder);
    return rowBuilder.build();
  }

}
