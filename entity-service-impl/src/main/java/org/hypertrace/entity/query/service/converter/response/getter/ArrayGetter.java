package org.hypertrace.entity.query.service.converter.response.getter;

import static org.hypertrace.entity.query.service.v1.ValueType.STRING;

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
  private final ValueHelper valueHelper;

  @Inject
  public ArrayGetter(
      @Named("nested_value") final ValueGetter nestedValueGetter,
      @Named("direct_value") final ValueGetter directValueGetter,
      final ValueHelper valueHelper) {
    this.nestedValueGetter = nestedValueGetter;
    this.directValueGetter = directValueGetter;
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

      final Value value;

      if (nestedValueGetter.matches(node)) {
        value = nestedValueGetter.getValue(node);
      } else if (directValueGetter.matches(node)) {
        value = directValueGetter.getValue(node);
      } else {
        throw new ConversionException(String.format("Unexpected node (%s) found", node));
      }

      values.add(value);
    }

    final ValueType primitiveType =
        values.stream().map(Value::getValueType).findFirst().orElse(STRING);
    final ValueType type = valueHelper.getArrayValueType(primitiveType);

    final Value.Builder valueBuilder = Value.newBuilder().setValueType(type);

    switch (type) {
      case STRING_ARRAY:
        values.stream().map(Value::getString).forEach(valueBuilder::addStringArray);
        break;

      case INT_ARRAY:
        values.stream().map(Value::getInt).forEach(valueBuilder::addIntArray);
        break;

      case LONG_ARRAY:
        values.stream().map(Value::getLong).forEach(valueBuilder::addLongArray);
        break;

      case FLOAT_ARRAY:
        values.stream().map(Value::getFloat).forEach(valueBuilder::addFloatArray);
        break;

      case DOUBLE_ARRAY:
        values.stream().map(Value::getDouble).forEach(valueBuilder::addDoubleArray);
        break;

      case BYTES_ARRAY:
        values.stream().map(Value::getBytes).forEach(valueBuilder::addBytesArray);
        break;

      case BOOLEAN_ARRAY:
        values.stream().map(Value::getBoolean).forEach(valueBuilder::addBooleanArray);
        break;

      default:
        throw new ConversionException(String.format("Unknown array type: %s", type));
    }

    return valueBuilder.build();
  }
}
