package org.hypertrace.entity.query.service.converter;

import static java.util.function.Predicate.not;
import static org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator.ATTRIBUTE_UPDATE_OPERATION_UNSPECIFIED;
import static org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator.ATTRIBUTE_UPDATE_OPERATOR_ADD_TO_LIST_IF_ABSENT;
import static org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator.ATTRIBUTE_UPDATE_OPERATOR_REMOVE_FROM_LIST;
import static org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator.ATTRIBUTE_UPDATE_OPERATOR_SET;
import static org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator.UNRECOGNIZED;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING_ARRAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
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
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class UpdateConverterTest {
  private static final Set<AttributeUpdateOperator> ARRAY_OPERATORS =
      Set.of(
          ATTRIBUTE_UPDATE_OPERATOR_ADD_TO_LIST_IF_ABSENT,
          ATTRIBUTE_UPDATE_OPERATOR_REMOVE_FROM_LIST);

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
  @ArgumentsSource(AllOperatorsProvider.class)
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
    when(mockEntityAttributeMapping.isMultiValued(requestContext, "columnName"))
        .thenReturn(ARRAY_OPERATORS.contains(operator));
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

  @Test
  void testConvert_setPrimitiveValueInArray_throwsConversionException() {
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
        .thenReturn(Optional.of("attributes.subDocPath"));
    when(mockEntityAttributeMapping.isMultiValued(requestContext, "columnName")).thenReturn(true);
    assertThrows(
        ConversionException.class, () -> updateConverter.convert(operation, requestContext));
  }

  @Test
  void testConvert_setArrayValueInPrimitive_throwsConversionException() {
    final AttributeUpdateOperation operation =
        AttributeUpdateOperation.newBuilder()
            .setAttribute(ColumnIdentifier.newBuilder().setColumnName("columnName"))
            .setOperator(ATTRIBUTE_UPDATE_OPERATOR_SET)
            .setValue(
                LiteralConstant.newBuilder()
                    .setValue(
                        Value.newBuilder().setValueType(STRING_ARRAY).addStringArray("value")))
            .build();
    final RequestContext requestContext = new RequestContext();
    when(mockEntityAttributeMapping.getDocStorePathByAttributeId(requestContext, "columnName"))
        .thenReturn(Optional.of("attributes.subDocPath"));
    when(mockEntityAttributeMapping.isMultiValued(requestContext, "columnName")).thenReturn(false);
    assertThrows(
        ConversionException.class, () -> updateConverter.convert(operation, requestContext));
  }

  @ParameterizedTest
  @ArgumentsSource(ArrayOperatorsProvider.class)
  void testConvert_addOrRemovePrimitiveValueInPrimitive_throwsConversionException(
      final AttributeUpdateOperator operator) {
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
    when(mockEntityAttributeMapping.isMultiValued(requestContext, "columnName")).thenReturn(false);
    assertThrows(
        ConversionException.class, () -> updateConverter.convert(operation, requestContext));
  }

  @ParameterizedTest
  @ArgumentsSource(ArrayOperatorsProvider.class)
  void testConvert_addOrRemoveArrayValueInPrimitive_throwsConversionException(
      final AttributeUpdateOperator operator) {
    final AttributeUpdateOperation operation =
        AttributeUpdateOperation.newBuilder()
            .setAttribute(ColumnIdentifier.newBuilder().setColumnName("columnName"))
            .setOperator(operator)
            .setValue(
                LiteralConstant.newBuilder()
                    .setValue(
                        Value.newBuilder().setValueType(STRING_ARRAY).addStringArray("value")))
            .build();
    final RequestContext requestContext = new RequestContext();
    when(mockEntityAttributeMapping.getDocStorePathByAttributeId(requestContext, "columnName"))
        .thenReturn(Optional.of("attributes.subDocPath"));
    when(mockEntityAttributeMapping.isMultiValued(requestContext, "columnName")).thenReturn(false);
    assertThrows(
        ConversionException.class, () -> updateConverter.convert(operation, requestContext));
  }

  private static class AllOperatorsProvider implements ArgumentsProvider {
    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext context) {
      return Arrays.stream(AttributeUpdateOperator.values())
          .filter(not(ATTRIBUTE_UPDATE_OPERATION_UNSPECIFIED::equals))
          .filter(not(UNRECOGNIZED::equals))
          .map(Arguments::of);
    }
  }

  private static class ArrayOperatorsProvider implements ArgumentsProvider {
    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext context) {
      return ARRAY_OPERATORS.stream().map(Arguments::of);
    }
  }
}
