package org.hypertrace.entity.query.service.converter.response;

import static org.hypertrace.entity.query.service.v1.ValueType.BOOL;
import static org.hypertrace.entity.query.service.v1.ValueType.INT;
import static org.hypertrace.entity.query.service.v1.ValueType.LONG_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.inject.Guice;
import java.io.IOException;
import java.util.List;
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
  private final DocumentConverter documentConverter = Guice.createInjector(new ConverterModule(null)).getInstance(DocumentConverter.class);

  @Test
  void testConvert() throws IOException, ConversionException {
    final String json = new String(getClass().getClassLoader().getResourceAsStream("response/nested_document.json").readAllBytes());
    final Document document = new JSONDocument(json);

    final ResultSetMetadata resultSetMetadata = ResultSetMetadata.newBuilder()
        .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("tenantId"))
        .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("entityId"))
        .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("Entity.status"))
        .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("attributes.something"))
        .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("attributes.updated_time"))
        .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("attributes.address_map.galaxy"))
        .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("attributes.address_map.planet"))
        .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("attributes.valueMap"))
        .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("attributes.nonExisting"))
        .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("attributes.valueList"))
        .build();

    final Row expectedRow = Row.newBuilder()
        .addColumn(Value.newBuilder().setValueType(STRING).setString("tenant-1"))
        .addColumn(Value.newBuilder().setValueType(STRING).setString("0215f3f6-55eb-4d95-a116-60f2962528ef"))
        .addColumn(Value.newBuilder().setValueType(STRING).setString("Red Shift"))
        .addColumn(Value.newBuilder().setValueType(BOOL).setBoolean(true))
        .addColumn(Value.newBuilder().setValueType(LONG_ARRAY).addAllLongArray(List.of(1643702835L, 1643702900L)))
        .addColumn(Value.newBuilder().setValueType(STRING).setString("Milky Way"))
        .addColumn(Value.newBuilder().setValueType(STRING).setString("Mars"))
        .addColumn(Value.newBuilder().setValueType(STRING).setString("The Future is Red!"))
        .addColumn(Value.getDefaultInstance())
        .addColumn(Value.newBuilder().setValueType(INT).setInt(2037))
        .build();

    final Row actualRow = documentConverter.convertToRow(document, resultSetMetadata);
    assertEquals(expectedRow, actualRow);
  }
}
