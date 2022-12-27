package org.hypertrace.entity.query.service.converter.response.getter;

import static org.hypertrace.entity.query.service.v1.ValueType.BOOLEAN_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.BYTES_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.DOUBLE_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.FLOAT_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.INT_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.LONG_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.VALUE_ARRAY;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;

@Singleton
public class ArrayGetter implements ValueGetter {
  private final ValueGetter nestedValueGetter;
  private final ValueGetter directValueGetter;
  private final List<ValueGetter> rootGetters;
  private final ValueHelper valueHelper;

  @Inject
  public ArrayGetter(
      @Named("nested_value") final ValueGetter nestedValueGetter,
      @Named("direct_value") final ValueGetter directValueGetter,
      @Named("root_getters") final List<ValueGetter> rootGetters,
      final ValueHelper valueHelper) {
    this.nestedValueGetter = nestedValueGetter;
    this.directValueGetter = directValueGetter;
    this.rootGetters = rootGetters;
    this.valueHelper = valueHelper;
  }

  @Override
  public boolean matches(final JsonNode jsonNode) {
    return jsonNode != null && jsonNode.isArray();
  }

  @Override
  public Value getValue(final JsonNode jsonNode) throws ConversionException {
    final Iterator<JsonNode> elements = jsonNode.elements();

    final List<Value> values = new ArrayList<>();

    while (elements.hasNext()) {
      final JsonNode node = elements.next();
      boolean valueSet = false;

      for (ValueGetter getter : rootGetters) {
        if (getter.matches(node)) {
          final Value value;
          value = getter.getValue(node);
          values.add(value);
          valueSet = true;
          break;
        }
      }

      if (!valueSet) {
        throw new ConversionException(String.format("Unexpected node (%s) found", node));
      }
    }

    final ValueType type = values.stream().map(Value::getValueType).findFirst().orElse(STRING);

    final Value.Builder valueBuilder = Value.newBuilder();
    switch (type) {
      case STRING:
        values.stream().map(Value::getString).forEach(valueBuilder::addStringArray);
        valueBuilder.setValueType(STRING_ARRAY);
        break;

      case INT:
        values.stream().map(Value::getInt).forEach(valueBuilder::addIntArray);
        valueBuilder.setValueType(INT_ARRAY);
        break;

      case LONG:
        values.stream().map(Value::getLong).forEach(valueBuilder::addLongArray);
        valueBuilder.setValueType(LONG_ARRAY);
        break;

      case FLOAT:
        values.stream().map(Value::getFloat).forEach(valueBuilder::addFloatArray);
        valueBuilder.setValueType(FLOAT_ARRAY);
        break;

      case DOUBLE:
        values.stream().map(Value::getDouble).forEach(valueBuilder::addDoubleArray);
        valueBuilder.setValueType(DOUBLE_ARRAY);
        break;

      case BYTES:
        values.stream().map(Value::getBytes).forEach(valueBuilder::addBytesArray);
        valueBuilder.setValueType(BYTES_ARRAY);
        break;

      case BOOL:
        values.stream().map(Value::getBoolean).forEach(valueBuilder::addBooleanArray);
        valueBuilder.setValueType(BOOLEAN_ARRAY);
        break;

      case INT_ARRAY:
        values.stream().map(Value::getIntArrayList).forEach(valueBuilder::addAllIntArray);
        valueBuilder.setValueType(VALUE_ARRAY);
        break;

      case LONG_ARRAY:
        values.stream().map(Value::getLongArrayList).forEach(valueBuilder::addAllLongArray);
        valueBuilder.setValueType(VALUE_ARRAY);
        break;

      case STRING_ARRAY:
        values.stream().map(Value::getStringArrayList).forEach(valueBuilder::addAllStringArray);
        valueBuilder.setValueType(VALUE_ARRAY);
        break;

      case FLOAT_ARRAY:
        values.stream().map(Value::getFloatArrayList).forEach(valueBuilder::addAllFloatArray);
        valueBuilder.setValueType(VALUE_ARRAY);
        break;

      case DOUBLE_ARRAY:
        values.stream().map(Value::getDoubleArrayList).forEach(valueBuilder::addAllDoubleArray);
        valueBuilder.setValueType(VALUE_ARRAY);
        break;

      case BYTES_ARRAY:
        values.stream().map(Value::getBytesArrayList).forEach(valueBuilder::addAllBytesArray);
        valueBuilder.setValueType(VALUE_ARRAY);
        break;

      case BOOLEAN_ARRAY:
        values.stream().map(Value::getBooleanArrayList).forEach(valueBuilder::addAllBooleanArray);
        valueBuilder.setValueType(VALUE_ARRAY);
        break;

      case STRING_MAP:
        values.stream().map(Value::getStringMapMap).forEach(valueBuilder::putAllStringMap);
        valueBuilder.setValueType(VALUE_ARRAY);
        break;

      case VALUE_MAP:
        values.stream().map(Value::getValueMapMap).forEach(valueBuilder::putAllValueMap);
        valueBuilder.setValueType(VALUE_ARRAY);
        break;

      case VALUE_ARRAY:
        values.stream().map(Value::getValueArrayList).forEach(valueBuilder::addAllValueArray);
        valueBuilder.setValueType(VALUE_ARRAY);
        break;

      default:
        throw new ConversionException(String.format("Unknown array type: %s", type));
    }

    return valueBuilder.build();
  }
}
