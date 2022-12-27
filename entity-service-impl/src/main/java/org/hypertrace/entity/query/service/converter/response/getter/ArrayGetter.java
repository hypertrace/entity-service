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
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;

@Singleton
public class ArrayGetter implements ValueGetter {
  private final List<ValueGetter> rootGetters;

  @Inject
  public ArrayGetter(@Named("root_getters") final List<ValueGetter> rootGetters) {
    this.rootGetters = rootGetters;
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

    final ValueType containingType =
        values.stream().map(Value::getValueType).findFirst().orElse(STRING);

    final Value.Builder valueBuilder = Value.newBuilder();
    switch (containingType) {
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
      case LONG_ARRAY:
      case STRING_ARRAY:
      case FLOAT_ARRAY:
      case DOUBLE_ARRAY:
      case BYTES_ARRAY:
      case BOOLEAN_ARRAY:
      case STRING_MAP:
      case VALUE_MAP:
      case VALUE_ARRAY:
        valueBuilder.addAllValueArray(values);
        valueBuilder.setValueType(VALUE_ARRAY);
        break;

      default:
        throw new ConversionException(String.format("Unknown array type: %s", containingType));
    }

    return valueBuilder.build();
  }
}
