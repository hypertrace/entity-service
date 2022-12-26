package org.hypertrace.entity.query.service.converter.response.getter;

import static java.util.Collections.emptyIterator;
import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUES_KEY;
import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUE_MAP_KEY;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING_MAP;
import static org.hypertrace.entity.query.service.v1.ValueType.VALUE_MAP;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;

@Singleton
public class MapValueGetter implements ValueGetter {
  private final ValueGetter nestedValueGetter;
  private final List<ValueGetter> rootGetters;
  private final OneOfAccessor<Value, ValueType> valueOneOfAccessor;
  private final ValueHelper valueHelper;

  @Inject
  public MapValueGetter(
      @Named("nested_value") final ValueGetter nestedValueGetter,
      @Named("root_getters") final List<ValueGetter> rootGetters,
      final OneOfAccessor<Value, ValueType> valueOneOfAccessor,
      final ValueHelper valueHelper) {
    this.nestedValueGetter = nestedValueGetter;
    this.rootGetters = rootGetters;
    this.valueOneOfAccessor = valueOneOfAccessor;
    this.valueHelper = valueHelper;
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

    if (valuesNode == null || !valuesNode.isObject()) {
      throw new ConversionException(
          String.format("Unexpected node (%s) found under %s", valuesNode, VALUE_MAP_KEY));
    }

    final JsonNode mapNode = valuesNode.get(VALUES_KEY);
    final Iterator<Entry<String, JsonNode>> fields =
        mapNode == null ? emptyIterator() : mapNode.fields();
    final Value.Builder valueBuilder = Value.newBuilder().setValueType(STRING_MAP);

    while (fields.hasNext()) {
      final Entry<String, JsonNode> entry = fields.next();

      final String key = entry.getKey();
      final JsonNode node = entry.getValue();
      boolean valueSet = false;
      if (nestedValueGetter.matches(node)) {
        final Value value = nestedValueGetter.getValue(node);
        Object obj = valueOneOfAccessor.access(value, value.getValueType());
        valueBuilder.putStringMap(key, obj.toString());
        continue;
      }
      for (ValueGetter getter : rootGetters) {
        if (getter.matches(node)) {
          final Value value = this.getValue(node);
          valueBuilder.setValueType(VALUE_MAP);
          valueBuilder.putValueMap(key, value);
          valueSet = true;
          break;
        }
      }
      if (!valueSet) {
        throw new ConversionException(
            String.format("Unexpected node (%s) found for key (%s)", node, key));
      }
    }

    return valueBuilder.build();
  }
}
