package org.hypertrace.entity.query.service.converter.response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.ColumnMetadata;
import org.hypertrace.entity.query.service.v1.ResultSetMetadata;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.Value;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class DocumentConverterImpl implements DocumentConverter {
  private final ValueMapper valueMapper;
  private final ObjectMapper objectMapper;

  @Override
  public Row convertToRow(final Document document, final ResultSetMetadata resultSetMetadata)
      throws ConversionException {
    final JsonNode jsonNode;

    try {
      jsonNode = objectMapper.readTree(document.toJson());
    } catch (JsonProcessingException e) {
      throw new ConversionException(
          String.format("Error converting document: %s", document.toJson()), e);
    }

    final Map<String, Value> valueMap = valueMapper.getMappedValues(jsonNode);

    return buildRow(resultSetMetadata, valueMap);
  }

  private Row buildRow(
      final ResultSetMetadata resultSetMetadata, final Map<String, Value> valueMap) {
    final Row.Builder builder = Row.newBuilder();
    final Value defaultValue = getNullPlaceholderValue();

    for (final ColumnMetadata columnMetadata : resultSetMetadata.getColumnMetadataList()) {
      final Value value = valueMap.getOrDefault(columnMetadata.getColumnName(), defaultValue);
      builder.addColumn(value);
    }

    return builder.build();
  }

  private Value getNullPlaceholderValue() {
    return Value.getDefaultInstance();
  }
}
