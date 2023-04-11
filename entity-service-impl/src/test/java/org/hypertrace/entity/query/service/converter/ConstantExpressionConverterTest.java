package org.hypertrace.entity.query.service.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.protobuf.ByteString;
import java.util.List;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.accessor.ValueOneOfAccessor;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.mockito.Mock;

class ConstantExpressionConverterTest {

  @Mock private EntityAttributeMapping mockEntityAttributeMapping;
  private final Converter<LiteralConstant, ConstantExpression> constantExpressionConverter =
      new ConstantExpressionConverter(
          new ValueHelper(new ValueOneOfAccessor(), mockEntityAttributeMapping));
  private final RequestContext requestContext = RequestContext.forTenantId("Martian");

  @Test
  void testConvert() throws ConversionException {
    assertEquals(
        ConstantExpression.of("Mars"),
        constantExpressionConverter.convert(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.STRING).setString("Mars"))
                .build(),
            requestContext));

    assertEquals(
        ConstantExpression.of(123_456_789_111L),
        constantExpressionConverter.convert(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.LONG).setLong(123_456_789_111L))
                .build(),
            requestContext));

    assertEquals(
        ConstantExpression.of(123_456_789),
        constantExpressionConverter.convert(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.INT).setInt(123_456_789))
                .build(),
            requestContext));

    assertEquals(
        ConstantExpression.of(123.78F),
        constantExpressionConverter.convert(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.FLOAT).setFloat(123.78F))
                .build(),
            requestContext));

    assertEquals(
        ConstantExpression.of(123.78D),
        constantExpressionConverter.convert(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(123.78))
                .build(),
            requestContext));

    assertEquals(
        ConstantExpression.of("A_signal_from_Mars"),
        constantExpressionConverter.convert(
            LiteralConstant.newBuilder()
                .setValue(
                    Value.newBuilder()
                        .setValueType(ValueType.BYTES)
                        .setBytes(ByteString.copyFromUtf8("A_signal_from_Mars")))
                .build(),
            requestContext));

    assertEquals(
        ConstantExpression.of(false),
        constantExpressionConverter.convert(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.BOOL).setBoolean(false))
                .build(),
            requestContext));

    assertEquals(
        ConstantExpression.of(1235L),
        constantExpressionConverter.convert(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.TIMESTAMP).setTimestamp(1235L))
                .build(),
            requestContext));

    assertEquals(
        ConstantExpression.ofStrings(List.of("Mars", "Venus")),
        constantExpressionConverter.convert(
            LiteralConstant.newBuilder()
                .setValue(
                    Value.newBuilder()
                        .setValueType(ValueType.STRING_ARRAY)
                        .addStringArray("Mars")
                        .addStringArray("Venus"))
                .build(),
            requestContext));

    assertEquals(
        ConstantExpression.ofNumbers(List.of(123_456_789_111L, 123_456_789_112L)),
        constantExpressionConverter.convert(
            LiteralConstant.newBuilder()
                .setValue(
                    Value.newBuilder()
                        .setValueType(ValueType.LONG_ARRAY)
                        .addLongArray(123_456_789_111L)
                        .addLongArray(123_456_789_112L))
                .build(),
            requestContext));

    assertEquals(
        ConstantExpression.ofNumbers(List.of(123_456_789, 123_456_790)),
        constantExpressionConverter.convert(
            LiteralConstant.newBuilder()
                .setValue(
                    Value.newBuilder()
                        .setValueType(ValueType.INT_ARRAY)
                        .addIntArray(123_456_789)
                        .addIntArray(123_456_790))
                .build(),
            requestContext));

    assertEquals(
        ConstantExpression.ofNumbers(List.of(123.78F, 125.78F)),
        constantExpressionConverter.convert(
            LiteralConstant.newBuilder()
                .setValue(
                    Value.newBuilder()
                        .setValueType(ValueType.FLOAT_ARRAY)
                        .addFloatArray(123.78F)
                        .addFloatArray(125.78F))
                .build(),
            requestContext));

    assertEquals(
        ConstantExpression.ofNumbers(List.of(123.78D, 223.78D)),
        constantExpressionConverter.convert(
            LiteralConstant.newBuilder()
                .setValue(
                    Value.newBuilder()
                        .setValueType(ValueType.DOUBLE_ARRAY)
                        .addDoubleArray(123.78)
                        .addDoubleArray(223.78))
                .build(),
            requestContext));

    assertEquals(
        ConstantExpression.ofStrings(List.of("A_signal_from_Mars", "Another_signal_from_Mars")),
        constantExpressionConverter.convert(
            LiteralConstant.newBuilder()
                .setValue(
                    Value.newBuilder()
                        .setValueType(ValueType.BYTES_ARRAY)
                        .addBytesArray(ByteString.copyFromUtf8("A_signal_from_Mars"))
                        .addBytesArray(ByteString.copyFromUtf8("Another_signal_from_Mars")))
                .build(),
            requestContext));

    assertEquals(
        ConstantExpression.ofBooleans(List.of(true, false)),
        constantExpressionConverter.convert(
            LiteralConstant.newBuilder()
                .setValue(
                    Value.newBuilder()
                        .setValueType(ValueType.BOOLEAN_ARRAY)
                        .addBooleanArray(true)
                        .addBooleanArray(false))
                .build(),
            requestContext));
  }

  @ParameterizedTest
  @EnumSource(
      value = ValueType.class,
      names = {
        "UNRECOGNIZED",
        "STRING_ARRAY",
        "LONG_ARRAY",
        "INT_ARRAY",
        "FLOAT_ARRAY",
        "DOUBLE_ARRAY",
        "BYTES_ARRAY",
        "BOOLEAN_ARRAY",
        "STRING_MAP",
        "VALUE_MAP",
        "VALUE_ARRAY"
      },
      mode = Mode.EXCLUDE)
  void testConvertCoverage(final ValueType valueType) throws ConversionException {
    assertNotNull(
        constantExpressionConverter.convert(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(valueType))
                .build(),
            requestContext));
  }
}
