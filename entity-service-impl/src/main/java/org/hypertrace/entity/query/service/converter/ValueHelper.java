package org.hypertrace.entity.query.service.converter;

import static com.google.common.base.Suppliers.memoize;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.entity.query.service.v1.ValueType.BOOL;
import static org.hypertrace.entity.query.service.v1.ValueType.BOOLEAN_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.BOOLEAN_MAP;
import static org.hypertrace.entity.query.service.v1.ValueType.BYTES;
import static org.hypertrace.entity.query.service.v1.ValueType.BYTES_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.BYTES_MAP;
import static org.hypertrace.entity.query.service.v1.ValueType.DOUBLE;
import static org.hypertrace.entity.query.service.v1.ValueType.DOUBLE_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.DOUBLE_MAP;
import static org.hypertrace.entity.query.service.v1.ValueType.FLOAT;
import static org.hypertrace.entity.query.service.v1.ValueType.FLOAT_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.FLOAT_MAP;
import static org.hypertrace.entity.query.service.v1.ValueType.INT;
import static org.hypertrace.entity.query.service.v1.ValueType.INT_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.INT_MAP;
import static org.hypertrace.entity.query.service.v1.ValueType.LONG;
import static org.hypertrace.entity.query.service.v1.ValueType.LONG_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.LONG_MAP;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING_MAP;
import static org.hypertrace.entity.query.service.v1.ValueType.TIMESTAMP;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.service.util.DocStoreJsonFormat;
import org.hypertrace.entity.service.util.DocStoreJsonFormat.Printer;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class ValueHelper {
  public static final String VALUES_KEY = "values";

  public static final String VALUE_KEY = "value";
  public static final String VALUE_LIST_KEY = "valueList";
  public static final String VALUE_MAP_KEY = "valueMap";

  private static final Printer PRINTER = DocStoreJsonFormat.printer();
  private static final String NULL_VALUE = "null";

  private static final Set<ValueType> PRIMITIVE_TYPES =
      Set.of(STRING, LONG, INT, FLOAT, DOUBLE, BYTES, BOOL, TIMESTAMP);

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

  private static final Supplier<Map<ValueType, String>> TYPE_TO_STRING_VALUE_MAP =
      memoize(ValueHelper::getTypeToStringValueMap);

  private static final Supplier<Map<ValueType, ValueType>> PRIMITIVE_TO_ARRAY_MAP =
      memoize(ValueHelper::getPrimitiveToArrayMap);
  private static final Supplier<Map<ValueType, ValueType>> PRIMITIVE_TO_MAP =
      memoize(ValueHelper::getPrimitiveToMap);
  private static final Supplier<Map<ValueType, ArrayToPrimitiveValuesConverter<?>>>
      ARRAY_TO_PRIMITIVE_CONVERTER_MAP = memoize(ValueHelper::getArrayToPrimitiveConverterMap);

  private static final Supplier<Map<String, ValueType>> STRING_VALUE_TO_PRIMITIVE_TYPE_MAP =
      memoize(ValueHelper::getStringValueToPrimitiveTypeMap);

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

  public boolean isNull(final Value value) {
    return NULL_VALUE.equalsIgnoreCase(value.getString());
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

  public SubDocumentValue convertToSubDocumentValue(final Value value) throws ConversionException {
    final ValueType type = value.getValueType();

    if (isArray(type)) {
      final List<Value> values = ARRAY_TO_PRIMITIVE_CONVERTER_MAP.get().get(type).apply(value);
      final List<Document> documents = new ArrayList<>();

      for (final Value singleValue : values) {
        documents.add(convertToDocument(singleValue));
      }

      return SubDocumentValue.of(documents);
    }

    if (isPrimitive(type)) {
      return SubDocumentValue.of(convertToDocument(value));
    }

    throw new ConversionException(String.format("Unsupported value type: %s", type));
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

  public String getStringValue(final ValueType valueType) throws ConversionException {
    final String type = TYPE_TO_STRING_VALUE_MAP.get().get(valueType);

    if (type == null) {
      throw new ConversionException(String.format("A suitable type not found for %s", valueType));
    }

    return type;
  }

  public ValueType getArrayValueType(final ValueType primitiveValueType)
      throws ConversionException {
    final ValueType arrayType = PRIMITIVE_TO_ARRAY_MAP.get().get(primitiveValueType);

    if (arrayType == null) {
      throw new ConversionException(
          String.format("A suitable array type not found for %s", primitiveValueType));
    }

    return arrayType;
  }

  public ValueType getMapValueType(final ValueType primitiveValueType) throws ConversionException {
    final ValueType mapType = PRIMITIVE_TO_MAP.get().get(primitiveValueType);

    if (mapType == null) {
      throw new ConversionException(
          String.format("A suitable map type not found for %s", primitiveValueType));
    }

    return mapType;
  }

  public ValueType getPrimitiveValueType(final String primitiveType) throws ConversionException {
    final ValueType arrayType = STRING_VALUE_TO_PRIMITIVE_TYPE_MAP.get().get(primitiveType);

    if (arrayType == null) {
      throw new ConversionException(
          String.format("A suitable array type not found for %s", primitiveType));
    }

    return arrayType;
  }

  private static Map<ValueType, String> getTypeToStringValueMap() {
    final Map<ValueType, String> map = new EnumMap<>(ValueType.class);

    // Primitives
    map.put(STRING, "string");
    map.put(LONG, "long");
    map.put(INT, "int");
    map.put(FLOAT, "float");
    map.put(DOUBLE, "double");
    map.put(BYTES, "bytes");
    map.put(BOOL, "boolean");
    map.put(TIMESTAMP, "timestamp");

    // Arrays
    map.put(STRING_ARRAY, "string");
    map.put(LONG_ARRAY, "long");
    map.put(INT_ARRAY, "int");
    map.put(FLOAT_ARRAY, "float");
    map.put(DOUBLE_ARRAY, "double");
    map.put(BYTES_ARRAY, "bytes");
    map.put(BOOLEAN_ARRAY, "boolean");

    // Maps
    map.put(STRING_MAP, "string");

    return unmodifiableMap(map);
  }

  private static Map<ValueType, ValueType> getPrimitiveToArrayMap() {
    final Map<ValueType, ValueType> map = new EnumMap<>(ValueType.class);

    map.put(STRING, STRING_ARRAY);
    map.put(LONG, LONG_ARRAY);
    map.put(INT, INT_ARRAY);
    map.put(FLOAT, FLOAT_ARRAY);
    map.put(DOUBLE, DOUBLE_ARRAY);
    map.put(BYTES, BYTES_ARRAY);
    map.put(BOOL, BOOLEAN_ARRAY);
    map.put(TIMESTAMP, LONG_ARRAY);

    return unmodifiableMap(map);
  }

  private static Map<ValueType, ValueType> getPrimitiveToMap() {
    final Map<ValueType, ValueType> map = new EnumMap<>(ValueType.class);

    map.put(STRING, STRING_MAP);
    map.put(LONG, LONG_MAP);
    map.put(INT, INT_MAP);
    map.put(FLOAT, FLOAT_MAP);
    map.put(DOUBLE, DOUBLE_MAP);
    map.put(BYTES, BYTES_MAP);
    map.put(BOOL, BOOLEAN_MAP);
    map.put(TIMESTAMP, LONG_MAP);

    return unmodifiableMap(map);
  }

  private static Map<ValueType, ArrayToPrimitiveValuesConverter<?>>
      getArrayToPrimitiveConverterMap() {
    final Map<ValueType, ArrayToPrimitiveValuesConverter<?>> map = new EnumMap<>(ValueType.class);

    map.put(
        STRING_ARRAY,
        new ArrayToPrimitiveValuesConverter<>(
            Value::getStringArrayList, Value.Builder::setString, STRING));
    map.put(
        LONG_ARRAY,
        new ArrayToPrimitiveValuesConverter<>(
            Value::getLongArrayList, Value.Builder::setLong, LONG));
    map.put(
        INT_ARRAY,
        new ArrayToPrimitiveValuesConverter<>(Value::getIntArrayList, Value.Builder::setInt, INT));
    map.put(
        FLOAT_ARRAY,
        new ArrayToPrimitiveValuesConverter<>(
            Value::getFloatArrayList, Value.Builder::setFloat, FLOAT));
    map.put(
        DOUBLE_ARRAY,
        new ArrayToPrimitiveValuesConverter<>(
            Value::getDoubleArrayList, Value.Builder::setDouble, DOUBLE));
    map.put(
        BYTES_ARRAY,
        new ArrayToPrimitiveValuesConverter<>(
            Value::getBytesArrayList, Value.Builder::setBytes, BYTES));
    map.put(
        BOOLEAN_ARRAY,
        new ArrayToPrimitiveValuesConverter<>(
            Value::getBooleanArrayList, Value.Builder::setBoolean, BOOL));

    return unmodifiableMap(map);
  }

  private static Map<String, ValueType> getStringValueToPrimitiveTypeMap() {
    final Map<String, ValueType> map = new HashMap<>();

    map.put("string", STRING);
    map.put("long", LONG);
    map.put("int", INT);
    map.put("float", FLOAT);
    map.put("double", DOUBLE);
    map.put("bytes", BYTES);
    map.put("boolean", BOOL);
    map.put("timestamp", TIMESTAMP);

    return unmodifiableMap(map);
  }

  private JSONDocument convertToDocument(final Value value) throws ConversionException {
    try {
      return new JSONDocument(PRINTER.print(LiteralConstant.newBuilder().setValue(value)));
    } catch (final IOException e) {
      throw new ConversionException(e.getMessage(), e);
    }
  }

  @AllArgsConstructor
  private static class ArrayToPrimitiveValuesConverter<T> {
    private final Function<Value, List<T>> arrayValueGetter;
    private final BiFunction<Value.Builder, T, Value.Builder> primitiveSetter;
    private final ValueType primitiveType;

    List<Value> apply(final Value arrayValue) {
      final List<T> listValue = arrayValueGetter.apply(arrayValue);
      final Value.Builder valueBuilder = Value.newBuilder().setValueType(primitiveType);
      return listValue.stream()
          .map(primitive -> primitiveSetter.apply(valueBuilder, primitive))
          .map(Value.Builder::build)
          .collect(toUnmodifiableList());
    }
  }
}
