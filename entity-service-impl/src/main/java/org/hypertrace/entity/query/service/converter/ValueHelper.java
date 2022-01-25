package org.hypertrace.entity.query.service.converter;

import static com.google.common.base.Suppliers.memoize;
import static java.util.Collections.unmodifiableMap;
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
import static org.hypertrace.entity.query.service.v1.ValueType.TIMESTAMP;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.protobuf.ByteString;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class ValueHelper {
  private static final Supplier<Map<ValueType, String>> SUFFIX_MAP =
      memoize(ValueHelper::getSuffixMap);

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

  public boolean isList(final ValueType valueType) {
    return ARRAY_TYPES.contains(valueType);
  }

  public boolean isMap(final ValueType valueType) {
    return MAP_TYPES.contains(valueType);
  }

  public <T> T getPrimitive(final Value value) throws ConversionException {
    final ValueType valueType = value.getValueType();

    if (!isPrimitive(valueType)) {
      throw new ConversionException(String.format("%s is not a primitive type", valueType));
    }

    return valueAccessor.access(value, valueType);
  }

  public <T> List<T> getList(final Value value) throws ConversionException {
    final ValueType valueType = value.getValueType();

    if (!isList(valueType)) {
      throw new ConversionException(String.format("%s is not a list type", valueType));
    }

    return valueAccessor.access(value, valueType);
  }

  public <T, U> Map<T, U> getMap(final Value value) throws ConversionException {
    final ValueType valueType = value.getValueType();

    if (!isMap(valueType)) {
      throw new ConversionException(String.format("%s is not a map type", valueType));
    }

    return valueAccessor.access(value, valueType);
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
        return ConstantExpression.of(value.getStringArrayList().get(index));

      case LONG_ARRAY:
        return ConstantExpression.of(value.getLongArrayList().get(index));

      case INT_ARRAY:
        return ConstantExpression.of(value.getIntArrayList().get(index));

      case FLOAT_ARRAY:
        return ConstantExpression.of(value.getFloatArrayList().get(index));

      case DOUBLE_ARRAY:
        return ConstantExpression.of(value.getDoubleArrayList().get(index));

      case BYTES_ARRAY:
        return ConstantExpression.of(
            new String(value.getBytesArrayList().get(index).toByteArray()));

      case BOOLEAN_ARRAY:
        return ConstantExpression.of(value.getBooleanArrayList().get(index));

      default:
        throw new ConversionException(String.format("Not a list type: %s", value.getValueType()));
    }
  }

  public String getFieldSuffix(final ValueType valueType) throws ConversionException {
    final String fieldSuffix = SUFFIX_MAP.get().get(valueType);

    if (fieldSuffix == null) {
      throw new ConversionException(String.format("Couldn't find suffix for type: %s", valueType));
    }

    return fieldSuffix;
  }

  private static Map<ValueType, String> getSuffixMap() {
    final Map<ValueType, String> map = new EnumMap<>(ValueType.class);

    map.put(STRING, ".value.string");
    map.put(LONG, ".value.long");
    map.put(INT, ".value.int");
    map.put(FLOAT, ".value.float");
    map.put(DOUBLE, ".value.double");
    map.put(BYTES, ".value.bytes");
    map.put(BOOL, ".value.bool");
    map.put(TIMESTAMP, ".value.timestamp");
    map.put(STRING_ARRAY, ".valueList.values.%d.value.string");
    map.put(LONG_ARRAY, ".valueList.values.%d.value.long");
    map.put(INT_ARRAY, ".valueList.values.%d.value.int");
    map.put(FLOAT_ARRAY, ".valueList.values.%d.value.float");
    map.put(DOUBLE_ARRAY, ".valueList.values.%d.value.double");
    map.put(BYTES_ARRAY, ".valueList.values.%d.value.bytes");
    map.put(BOOLEAN_ARRAY, ".valueList.values.%d.value.bool");
    map.put(STRING_MAP, ".valueMap.values.%s.value.string");

    return unmodifiableMap(map);
  }
}
