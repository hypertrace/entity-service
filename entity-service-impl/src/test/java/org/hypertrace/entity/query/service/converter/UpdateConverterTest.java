package org.hypertrace.entity.query.service.converter;

import static org.hypertrace.entity.query.service.v1.ValueType.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.hypertrace.core.documentstore.model.subdoc.PrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.accessor.ValueOneOfAccessor;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierConversionMetadata;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierConverter;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierConverterFactory;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.SetAttribute;
import org.hypertrace.entity.query.service.v1.UpdateOperation;
import org.hypertrace.entity.query.service.v1.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class UpdateConverterTest {

  @Mock private EntityAttributeMapping mockEntityAttributeMapping;
  @Mock private IdentifierConverterFactory mockIdentifierConverterFactory;
  @Mock private IdentifierConverter mockIdentifierConverter;

  private UpdateConverter updateConverter;

  @BeforeEach
  void setUp() {
    updateConverter =
        new UpdateConverter(
            mockEntityAttributeMapping,
            mockIdentifierConverterFactory,
            new ValueHelper(new ValueOneOfAccessor()));
  }

  @Test
  void testConvert() throws Exception {
    final UpdateOperation operation =
        UpdateOperation.newBuilder()
            .setSetAttribute(
                SetAttribute.newBuilder()
                    .setAttribute(ColumnIdentifier.newBuilder().setColumnName("columnName"))
                    .setValue(
                        LiteralConstant.newBuilder()
                            .setValue(Value.newBuilder().setValueType(STRING).setString("value"))))
            .build();
    final RequestContext requestContext = new RequestContext();
    final SubDocumentUpdate expectedResult =
        SubDocumentUpdate.of("attributes.sub_doc_path.value.string", "value");
    when(mockEntityAttributeMapping.getDocStorePathByAttributeId(requestContext, "columnName"))
        .thenReturn(Optional.of("subDocPath"));
    when(mockIdentifierConverterFactory.getIdentifierConverter(
            "columnName", "subDocPath", STRING, requestContext))
        .thenReturn(mockIdentifierConverter);
    when(mockIdentifierConverter.convert(
            IdentifierConversionMetadata.builder()
                .valueType(STRING)
                .subDocPath("subDocPath")
                .build(),
            requestContext))
        .thenReturn("attributes.sub_doc_path.value.string");
    final SubDocumentUpdate result = updateConverter.convert(operation, requestContext);

    assertEquals(expectedResult.getSubDocument().getPath(), result.getSubDocument().getPath());
    assertEquals(
        ((PrimitiveSubDocumentValue) expectedResult.getSubDocumentValue()).getValue().toString(),
        ((PrimitiveSubDocumentValue) result.getSubDocumentValue()).getValue().toString());
  }

  @Test
  void testConvert_IdentifierConverterFactoryThrowsConversionException() {
    final UpdateOperation operation = UpdateOperation.newBuilder().build();
    final RequestContext requestContext = new RequestContext();

    assertThrows(
        ConversionException.class, () -> updateConverter.convert(operation, requestContext));
  }
}
