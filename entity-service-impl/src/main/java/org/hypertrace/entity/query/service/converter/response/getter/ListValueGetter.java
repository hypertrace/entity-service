package org.hypertrace.entity.query.service.converter.response.getter;

import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUES_KEY;
import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUE_LIST_KEY;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING_ARRAY;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import java.util.List;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.Value;

@Singleton
public class ListValueGetter implements ValueGetter {
  private final ValueGetter arrayGetter;
  private final List<ValueGetter> rootGetters;

  @Inject
  public ListValueGetter(
      @Named("array") final ValueGetter arrayGetter,
      @Named("root_getters") final List<ValueGetter> rootGetters) {
    this.arrayGetter = arrayGetter;
    this.rootGetters = rootGetters;
  }

  @Override
  public boolean matches(JsonNode jsonNode) {
    return jsonNode != null
        && jsonNode.isObject()
        && jsonNode.has(VALUE_LIST_KEY)
        && jsonNode.get(VALUE_LIST_KEY).isObject();
  }

  @Override
  public Value getValue(final JsonNode jsonNode) throws ConversionException {
    final JsonNode valuesNode = jsonNode.get(VALUE_LIST_KEY);

    if (valuesNode == null || !valuesNode.isObject()) {
      throw new ConversionException(
          String.format("Unexpected node (%s) found under %s", valuesNode, VALUE_LIST_KEY));
    }

    final JsonNode listNode = valuesNode.get(VALUES_KEY);

    if (listNode == null) {
      return Value.newBuilder().setValueType(STRING_ARRAY).build();
    }
    if (arrayGetter.matches(listNode)) {
      return arrayGetter.getValue(listNode);
    } else {
      throw new ConversionException(String.format("Expected node (%s) to be an array", listNode));
    }
  }
}
