package org.hypertrace.entity.query.service.converter.response;

import static org.hypertrace.entity.query.service.v1.ValueType.BOOL;
import static org.hypertrace.entity.query.service.v1.ValueType.INT_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.LONG_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING_MAP;
import static org.hypertrace.entity.query.service.v1.ValueType.VALUE_MAP;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.inject.Guice;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ConverterModule;
import org.hypertrace.entity.query.service.v1.ColumnMetadata;
import org.hypertrace.entity.query.service.v1.ResultSetMetadata;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.Value;
import org.junit.jupiter.api.Test;

class DocumentConverterTest {
  private final DocumentConverter documentConverter =
      Guice.createInjector(new ConverterModule(null)).getInstance(DocumentConverter.class);

  @Test
  void testConvert() throws IOException, ConversionException {
    final String json =
        new String(
            getClass()
                .getClassLoader()
                .getResourceAsStream("response/nested_document.json")
                .readAllBytes());
    final Document document = new JSONDocument(json);

    final ResultSetMetadata resultSetMetadata =
        ResultSetMetadata.newBuilder()
            .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("tenantId"))
            .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("entityId"))
            .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("Entity.status"))
            .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("attributes.something"))
            .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("attributes.updated_time"))
            .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("attributes.address_map"))
            .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("attributes.valueMap"))
            .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("attributes.nonExisting"))
            .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("attributes.valueList"))
            .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("attributes.emptyMap"))
            .addColumnMetadata(
                ColumnMetadata.newBuilder().setColumnName("attributes.emptyMapNested"))
            .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("attributes.emptyList"))
            .addColumnMetadata(
                ColumnMetadata.newBuilder().setColumnName("attributes.emptyListNested"))
            .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("timestamp_2"))
            .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("countries"))
            .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("area"))
            .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("timestamp_1"))
            .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("region"))
            .build();

    final Row expectedRow =
        Row.newBuilder()
            .addColumn(Value.newBuilder().setValueType(STRING).setString("tenant-1"))
            .addColumn(
                Value.newBuilder()
                    .setValueType(STRING)
                    .setString("0215f3f6-55eb-4d95-a116-60f2962528ef"))
            .addColumn(Value.newBuilder().setValueType(STRING).setString("Red Shift"))
            .addColumn(Value.newBuilder().setValueType(BOOL).setBoolean(true))
            .addColumn(
                Value.newBuilder()
                    .setValueType(LONG_ARRAY)
                    .addAllLongArray(List.of(1643702835L, 1643702900L)))
            .addColumn(
                Value.newBuilder()
                    .setValueType(STRING_MAP)
                    .putAllStringMap(Map.of("galaxy", "Milky Way", "planet", "Mars")))
            .addColumn(Value.newBuilder().setValueType(STRING).setString("The Future is Red!"))
            .addColumn(Value.getDefaultInstance())
            .addColumn(
                Value.newBuilder()
                    .setValueType(INT_ARRAY)
                    .addAllIntArray(List.of(2016, 2037, 2122)))
            .addColumn(Value.newBuilder().setValueType(STRING_MAP).build())
            .addColumn(Value.newBuilder().setValueType(STRING_MAP).build())
            .addColumn(Value.newBuilder().setValueType(STRING_ARRAY).build())
            .addColumn(Value.newBuilder().setValueType(STRING_ARRAY).build())
            .addColumn(
                Value.newBuilder()
                    .setValueType(VALUE_MAP)
                    .putValueMap(
                        "last_activity_timestamp",
                        Value.newBuilder()
                            .setValueType(STRING_MAP)
                            .putStringMap("seconds", "10")
                            .putStringMap("nanos", "10")
                            .build())
                    .build())
            .addColumn(
                Value.newBuilder()
                    .setValueType(STRING_ARRAY)
                    .addAllStringArray(List.of("india", "australia")))
            .addColumn(
                Value.newBuilder().setValueType(INT_ARRAY).addAllIntArray(List.of(1, 2)).build())
            .addColumn(
                Value.newBuilder().setValueType(STRING_MAP).putStringMap("seconds", "10").build())
            .addColumn(
                Value.newBuilder()
                    .setValueType(VALUE_MAP)
                    .putValueMap(
                        "countries",
                        Value.newBuilder()
                            .setValueType(STRING_ARRAY)
                            .addAllStringArray(List.of("india", "australia"))
                            .build())
                    .build())
            .build();

    final Row actualRow = documentConverter.convertToRow(document, resultSetMetadata);
    assertEquals(expectedRow, actualRow);
  }
}
