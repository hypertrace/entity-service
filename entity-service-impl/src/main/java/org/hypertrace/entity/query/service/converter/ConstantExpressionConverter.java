package org.hypertrace.entity.query.service.converter;

import static java.util.stream.Collectors.toUnmodifiableList;

import com.google.inject.Singleton;
import com.google.protobuf.ByteString;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Value;

@Singleton
public class ConstantExpressionConverter implements Converter<LiteralConstant, ConstantExpression> {

  @Override
  public ConstantExpression convert(
      final LiteralConstant literalConstant, final RequestContext requestContext)
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
        return ConstantExpression.of(new String(value.getBytes().toByteArray()));

      case BOOL:
        return ConstantExpression.of(value.getBoolean());

      case TIMESTAMP:
        return ConstantExpression.of(value.getTimestamp());

      case STRING_ARRAY:
        if (value.getStringArrayList().isEmpty()) {
          return ConstantExpression.of((String) null);
        } else {
          return ConstantExpression.ofStrings(value.getStringArrayList());
        }

      case LONG_ARRAY:
        if (value.getLongArrayList().isEmpty()) {
          return ConstantExpression.of((Long) null);
        } else {
          return ConstantExpression.ofNumbers(value.getLongArrayList());
        }

      case INT_ARRAY:
        if (value.getIntArrayList().isEmpty()) {
          return ConstantExpression.of((Integer) null);
        } else {
          return ConstantExpression.ofNumbers(value.getIntArrayList());
        }

      case FLOAT_ARRAY:
        if (value.getFloatArrayList().isEmpty()) {
          return ConstantExpression.of((Float) null);
        } else {
          return ConstantExpression.ofNumbers(value.getFloatArrayList());
        }

      case DOUBLE_ARRAY:
        if (value.getDoubleArrayList().isEmpty()) {
          return ConstantExpression.of((Double) null);
        } else {
          return ConstantExpression.ofNumbers(value.getDoubleArrayList());
        }

      case BYTES_ARRAY:
        if (value.getBytesArrayList().isEmpty()) {
          return ConstantExpression.of((String) null);
        } else {
          return ConstantExpression.ofStrings(
              value.getBytesArrayList().stream()
                  .map(ByteString::toByteArray)
                  .map(String::new)
                  .collect(toUnmodifiableList()));
        }

      case BOOLEAN_ARRAY:
        if (value.getBooleanArrayList().isEmpty()) {
          return ConstantExpression.of((Boolean) null);
        } else {
          return ConstantExpression.ofBooleans(value.getBooleanArrayList());
        }

      case STRING_MAP:
        // TODO: See how this can be handled better
        if (value.getStringMapMap().isEmpty()) {
          return ConstantExpression.of((String) null);
        } else {
          return ConstantExpression.of(value.getStringMapMap().toString());
        }

      case UNRECOGNIZED:
      default:
        throw new ConversionException(
            String.format("Unknown value type: %s", value.getValueType()));
    }
  }
}
