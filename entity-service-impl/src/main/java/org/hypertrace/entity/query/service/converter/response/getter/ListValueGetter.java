package org.hypertrace.entity.query.service.converter.response.getter;

import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUES_KEY;
import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUE_LIST_KEY;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.Value;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class ListValueGetter implements ValueGetter {
  private final ArrayGetter arrayGetter;

  @Override
  public boolean matches(JsonNode jsonNode) {
    return jsonNode != null && jsonNode.isObject() && jsonNode.has(VALUE_LIST_KEY) && jsonNode.get(VALUE_LIST_KEY).isObject() && jsonNode.get(VALUE_LIST_KEY).has(VALUES_KEY) && jsonNode.get(VALUE_LIST_KEY).get(VALUES_KEY).isArray();
  }

  @Override
  public Value getValue(final JsonNode jsonNode) throws ConversionException {
    final JsonNode valuesNode = jsonNode.get(VALUE_LIST_KEY);

    if (valuesNode == null || !valuesNode.isObject() || !valuesNode.has(VALUES_KEY)) {
      throw new ConversionException(
          String.format("Unexpected node (%s) found under %s", valuesNode, VALUE_LIST_KEY));
    }

    final JsonNode listNode = valuesNode.get(VALUES_KEY);

    if (!arrayGetter.matches(listNode)) {
      throw new ConversionException(String.format("Expected node (%s) to be an array", listNode));
    }

    return arrayGetter.getValue(listNode);
  }
}
