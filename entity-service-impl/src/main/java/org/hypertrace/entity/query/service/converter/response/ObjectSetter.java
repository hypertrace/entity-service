package org.hypertrace.entity.query.service.converter.response;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.response.getter.ValueGetter;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.Value;

@Singleton
public class ObjectSetter {
  private final List<ValueGetter> rootGetters;

  @Inject
  public ObjectSetter(@Named("root_getters") final List<ValueGetter> rootGetters) {
    this.rootGetters = rootGetters;
  }

  public void setValue(final JsonNode jsonNode, final Row.Builder rowBuilder)
      throws ConversionException {
    final Iterator<Entry<String, JsonNode>> fields = jsonNode.fields();

    while (fields.hasNext()) {
      final Entry<String, JsonNode> entry = fields.next();
      final JsonNode node = entry.getValue();
      boolean valueSet = false;

      for (final ValueGetter getter : rootGetters) {
        if (getter.matches(node)) {
          final Value value = getter.getValue(node);
          addValue(value, rowBuilder);
          valueSet = true;
          break;
        }
      }

      if (!valueSet && node.isObject()) {
        setValue(node, rowBuilder);
      }
    }
  }

  private void addValue(final Value value, final Row.Builder rowBuilder) {
    rowBuilder.addColumn(value);
  }
}
