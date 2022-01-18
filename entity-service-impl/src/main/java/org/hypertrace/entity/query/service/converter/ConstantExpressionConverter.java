package org.hypertrace.entity.query.service.converter;

import static java.util.stream.Collectors.toUnmodifiableList;

import com.google.inject.Singleton;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Value;

@Singleton
public class ConstantExpressionConverter implements Converter<LiteralConstant, ConstantExpression> {

  @Override
  public ConstantExpression convert(final LiteralConstant literalConstant)
      throws ConversionException {
    final Value value = literalConstant.getValue();

    switch (value.getValueType()) {
      case STRING:
        return ConstantExpression.of(value.getString());

      case LONG:
        return ConstantExpression.of(value.getLong());

      case INT:
        return ConstantExpression.of(value.getInt());

      case FLOAT:
        return ConstantExpression.of(value.getFloat());

      case DOUBLE:
        return ConstantExpression.of(value.getDouble());

      case BYTES:
        return ConstantExpression.of(value.getBytes().toString());

      case BOOL:
        return ConstantExpression.of(value.getBoolean());

      case TIMESTAMP:
        return ConstantExpression.of(value.getTimestamp());

      case STRING_ARRAY:
        return ConstantExpression.ofStrings(value.getStringArrayList());

      case LONG_ARRAY:
        return ConstantExpression.ofNumbers(value.getLongArrayList());

      case INT_ARRAY:
        return ConstantExpression.ofNumbers(value.getIntArrayList());

      case FLOAT_ARRAY:
        return ConstantExpression.ofNumbers(value.getFloatArrayList());

      case DOUBLE_ARRAY:
        return ConstantExpression.ofNumbers(value.getDoubleArrayList());

      case BYTES_ARRAY:
        return ConstantExpression.ofStrings(
            value.getBytesArrayList().stream().map(Object::toString).collect(toUnmodifiableList()));

      case BOOLEAN_ARRAY:
        return ConstantExpression.ofBooleans(value.getBooleanArrayList());

      case STRING_MAP:
        // TODO: See how this can be handled better
        return ConstantExpression.of(value.getStringMapMap().toString());

      case UNRECOGNIZED:
      default:
        throw new ConversionException(
            String.format("Unknown value type: %s", value.getValueType()));
    }
  }
}
