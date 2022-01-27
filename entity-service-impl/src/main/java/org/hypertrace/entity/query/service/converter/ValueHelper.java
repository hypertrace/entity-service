package org.hypertrace.entity.query.service.converter;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.entity.query.service.v1.ValueType.BOOL;
import static org.hypertrace.entity.query.service.v1.ValueType.BOOLEAN_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.BYTES;
import static org.hypertrace.entity.query.service.v1.ValueType.BYTES_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.DOUBLE;
import static org.hypertrace.entity.query.service.v1.ValueType.DOUBLE_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.FLOAT;
import static org.hypertrace.entity.query.service.v1.ValueType.FLOAT_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.INT;
import static org.hypertrace.entity.query.service.v1.ValueType.INT_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.LONG;
import static org.hypertrace.entity.query.service.v1.ValueType.LONG_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING_MAP;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.protobuf.ByteString;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class ValueHelper {

  private static final Set<ValueType> PRIMITIVE_TYPES =
      Set.of(STRING, LONG, INT, FLOAT, DOUBLE, BYTES, BOOL);

  private static final Set<ValueType> ARRAY_TYPES =
      Set.of(
          STRING_ARRAY,
          LONG_ARRAY,
          INT_ARRAY,
          FLOAT_ARRAY,
          DOUBLE_ARRAY,
          BYTES_ARRAY,
          BOOLEAN_ARRAY);

  private static final Set<ValueType> MAP_TYPES = Set.of(STRING_MAP);

  private final OneOfAccessor<Value, ValueType> valueAccessor;

  public boolean isPrimitive(final ValueType valueType) {
    return PRIMITIVE_TYPES.contains(valueType);
  }

  public boolean isArray(final ValueType valueType) {
    return ARRAY_TYPES.contains(valueType);
  }

  public boolean isMap(final ValueType valueType) {
    return MAP_TYPES.contains(valueType);
  }

  public ConstantExpression convertToConstantExpression(final Value value)
      throws ConversionException {
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
            value.getBytesArrayList().stream()
                .map(ByteString::toByteArray)
                .map(String::new)
                .collect(toUnmodifiableList()));

      case BOOLEAN_ARRAY:
        return ConstantExpression.ofBooleans(value.getBooleanArrayList());

      case STRING_MAP:
      case UNRECOGNIZED:
      default:
        throw new ConversionException(
            String.format("Unsupported value type: %s", value.getValueType()));
    }
  }

  public ConstantExpression convertToConstantExpression(final Value value, final int index)
      throws ConversionException {
    switch (value.getValueType()) {
      case STRING_ARRAY:
      case BYTES_ARRAY:
        return ConstantExpression.of(
            valueAccessor.<String>accessListElement(value, value.getValueType(), index));

      case LONG_ARRAY:
      case INT_ARRAY:
      case FLOAT_ARRAY:
      case DOUBLE_ARRAY:
        return ConstantExpression.of(
            valueAccessor.<Number>accessListElement(value, value.getValueType(), index));

      case BOOLEAN_ARRAY:
        return ConstantExpression.of(
            valueAccessor.<Boolean>accessListElement(value, value.getValueType(), index));

      default:
        throw new ConversionException(String.format("Not a list type: %s", value.getValueType()));
    }
  }

  public <K> ConstantExpression convertToConstantExpression(final Value value, final K key)
      throws ConversionException {
    switch (value.getValueType()) {
      case STRING_MAP:
        return ConstantExpression.of(
            valueAccessor.<K, String>accessMapValue(value, value.getValueType(), key));

      default:
        throw new ConversionException(String.format("Not a map type: %s", value.getValueType()));
    }
  }
}
