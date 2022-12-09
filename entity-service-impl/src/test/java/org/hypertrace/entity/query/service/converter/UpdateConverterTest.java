package org.hypertrace.entity.query.service.converter;

import static org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator.ATTRIBUTE_UPDATE_OPERATOR_SET;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.accessor.ValueOneOfAccessor;
import org.hypertrace.entity.query.service.v1.AttributeUpdateOperation;
import org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class UpdateConverterTest {

  @Mock private EntityAttributeMapping mockEntityAttributeMapping;

  private UpdateConverter updateConverter;

  @BeforeEach
  void setUp() {
    updateConverter =
        new UpdateConverter(mockEntityAttributeMapping, new ValueHelper(new ValueOneOfAccessor()));
  }

  @Test
  void testConvert() throws Exception {
    final AttributeUpdateOperation operation =
        AttributeUpdateOperation.newBuilder()
            .setAttribute(ColumnIdentifier.newBuilder().setColumnName("columnName"))
            .setOperator(ATTRIBUTE_UPDATE_OPERATOR_SET)
            .setValue(
                LiteralConstant.newBuilder()
                    .setValue(Value.newBuilder().setValueType(STRING).setString("value")))
            .build();
    final RequestContext requestContext = new RequestContext();
    final SubDocumentUpdate expectedResult =
        SubDocumentUpdate.of(
            "attributes.subDocPath",
            SubDocumentValue.of(new JSONDocument("{\"value\":{\"string\":\"value\"}}")));
    when(mockEntityAttributeMapping.getDocStorePathByAttributeId(requestContext, "columnName"))
        .thenReturn(Optional.of("attributes.subDocPath"));
    final SubDocumentUpdate result = updateConverter.convert(operation, requestContext);

    assertEquals(expectedResult, result);
  }

  @ParameterizedTest
  @EnumSource(
      value = AttributeUpdateOperator.class,
      mode = Mode.EXCLUDE,
      names = {"ATTRIBUTE_UPDATE_OPERATION_UNSPECIFIED", "UNRECOGNIZED"})
  void testOperatorCoverage(final AttributeUpdateOperator operator) throws Exception {
    final AttributeUpdateOperation operation =
        AttributeUpdateOperation.newBuilder()
            .setAttribute(ColumnIdentifier.newBuilder().setColumnName("columnName"))
            .setOperator(operator)
            .setValue(
                LiteralConstant.newBuilder()
                    .setValue(Value.newBuilder().setValueType(STRING).setString("value")))
            .build();
    final RequestContext requestContext = new RequestContext();
    when(mockEntityAttributeMapping.getDocStorePathByAttributeId(requestContext, "columnName"))
        .thenReturn(Optional.of("attributes.subDocPath"));
    final SubDocumentUpdate result = updateConverter.convert(operation, requestContext);
    assertNotNull(result);
  }

  @Test
  void testConvertNonAttributeValue() {
    final AttributeUpdateOperation operation =
        AttributeUpdateOperation.newBuilder()
            .setAttribute(ColumnIdentifier.newBuilder().setColumnName("columnName"))
            .setOperator(ATTRIBUTE_UPDATE_OPERATOR_SET)
            .setValue(
                LiteralConstant.newBuilder()
                    .setValue(Value.newBuilder().setValueType(STRING).setString("value")))
            .build();
    final RequestContext requestContext = new RequestContext();
    when(mockEntityAttributeMapping.getDocStorePathByAttributeId(requestContext, "columnName"))
        .thenReturn(Optional.empty());
    assertThrows(
        ConversionException.class, () -> updateConverter.convert(operation, requestContext));
  }

  @Test
  void testConvert_IdentifierConverterFactoryThrowsConversionException() {
    final AttributeUpdateOperation operation =
        AttributeUpdateOperation.newBuilder()
            .setAttribute(ColumnIdentifier.newBuilder().setColumnName("columnName"))
            .setValue(
                LiteralConstant.newBuilder()
                    .setValue(Value.newBuilder().setValueType(STRING).setString("value")))
            .build();
    final RequestContext requestContext = new RequestContext();

    assertThrows(
        ConversionException.class, () -> updateConverter.convert(operation, requestContext));
  }
}
