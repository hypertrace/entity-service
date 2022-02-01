package org.hypertrace.entity.query.service.converter.response.getter;

import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUES_KEY;
import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUE_LIST_KEY;
import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUE_MAP_KEY;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING_MAP;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import java.util.Iterator;
import java.util.Map.Entry;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.Value;

@Singleton
public class MapValueGetter implements ValueGetter {
  private final ValueGetter nestedValueGetter;

  @Inject
  public MapValueGetter(@Named("nested_value") final ValueGetter nestedValueGetter) {
    this.nestedValueGetter = nestedValueGetter;
  }

  @Override
  public boolean matches(final JsonNode jsonNode) {
    return jsonNode != null
        && jsonNode.isObject()
        && jsonNode.has(VALUE_MAP_KEY)
        && jsonNode.get(VALUE_MAP_KEY).isObject();
  }

  @Override
  public Value getValue(final JsonNode jsonNode) throws ConversionException {
    final JsonNode valuesNode = jsonNode.get(VALUE_MAP_KEY);

    if (valuesNode == null || !valuesNode.isObject() || !valuesNode.has(VALUES_KEY)) {
      throw new ConversionException(
          String.format("Unexpected node (%s) found under %s", valuesNode, VALUE_LIST_KEY));
    }

    final JsonNode mapNode = valuesNode.get(VALUES_KEY);
    final Iterator<Entry<String, JsonNode>> fields = mapNode.fields();
    final Value.Builder valueBuilder = Value.newBuilder().setValueType(STRING_MAP);

    while (fields.hasNext()) {
      final Entry<String, JsonNode> entry = fields.next();

      final String key = entry.getKey();
      final JsonNode node = entry.getValue();

      if (!nestedValueGetter.matches(node)) {
        throw new ConversionException(
            String.format("Unexpected node (%s) found for key (%s)", node, key));
      }

      final Value value = nestedValueGetter.getValue(node);

      if (value.getValueType() != STRING) {
        throw new ConversionException(
            String.format("Maps of type %s are not supported yet", value.getValueType()));
      }

      valueBuilder.putStringMap(key, value.getString());
    }

    return valueBuilder.build();
  }
}
