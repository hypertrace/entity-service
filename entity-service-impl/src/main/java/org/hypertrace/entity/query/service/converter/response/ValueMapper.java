package org.hypertrace.entity.query.service.converter.response;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.response.getter.ValueGetter;
import org.hypertrace.entity.query.service.v1.Value;

@Singleton
public class ValueMapper {
  private static final Joiner DOT_JOINER = Joiner.on('.');

  private final List<ValueGetter> rootGetters;

  @Inject
  public ValueMapper(@Named("root_getters") final List<ValueGetter> rootGetters) {
    this.rootGetters = rootGetters;
  }

  public Map<String, Value> getMappedValues(final JsonNode jsonNode) throws ConversionException {
    return getMappedValuesInternal(jsonNode, null);
  }

  private Map<String, Value> getMappedValuesInternal(final JsonNode jsonNode, final String prefix)
      throws ConversionException {
    final Iterator<Entry<String, JsonNode>> fields = jsonNode.fields();
    final Map<String, Value> valueMap = new HashMap<>();

    while (fields.hasNext()) {
      final Entry<String, JsonNode> entry = fields.next();
      final JsonNode node = entry.getValue();
      String key = entry.getKey();

      if (prefix != null) {
        key = DOT_JOINER.join(prefix, key);
      }

      boolean valueSet = false;

      for (final ValueGetter getter : rootGetters) {
        if (getter.matches(node)) {
          final Value value = getter.getValue(node);
          valueMap.put(key, value);
          valueSet = true;
          break;
        }
      }

      if (!valueSet && node.isObject()) {
        final Map<String, Value> newMap = getMappedValuesInternal(node, key);
        valueMap.putAll(newMap);
      }
    }

    return valueMap;
  }
}
