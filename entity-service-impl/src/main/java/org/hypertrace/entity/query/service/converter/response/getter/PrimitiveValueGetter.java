package org.hypertrace.entity.query.service.converter.response.getter;

import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUE_KEY;
import static org.hypertrace.entity.query.service.v1.ValueType.BOOL;
import static org.hypertrace.entity.query.service.v1.ValueType.BYTES;
import static org.hypertrace.entity.query.service.v1.ValueType.DOUBLE;
import static org.hypertrace.entity.query.service.v1.ValueType.FLOAT;
import static org.hypertrace.entity.query.service.v1.ValueType.INT;
import static org.hypertrace.entity.query.service.v1.ValueType.LONG;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING;
import static org.hypertrace.entity.query.service.v1.ValueType.TIMESTAMP;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class PrimitiveValueGetter implements ValueGetter {
  private final ValueHelper valueHelper;

  private final BooleanGetter booleanGetter;
  private final BytesGetter bytesGetter;
  private final DoubleGetter doubleGetter;
  private final FloatGetter floatGetter;
  private final IntegerGetter intGetter;
  private final LongGetter longGetter;
  private final StringGetter stringGetter;

  private final Supplier<Map<ValueType, ValueGetter>> getterMap = Suppliers.memoize(this::getTypeToGetterMap);

  @Override
  public boolean matches(final JsonNode jsonNode) {
    return jsonNode != null && jsonNode.isObject() && jsonNode.has(VALUE_KEY);
  }

  @Override
  public Value getValue(final JsonNode jsonNode) throws ConversionException {
    final JsonNode typeNode = jsonNode.get(VALUE_KEY);

    if (typeNode == null || !typeNode.isObject()) {
      throw new ConversionException(String.format("Type value missing for node: %s", jsonNode));
    }

    final List<Entry<String, JsonNode>> fields = Lists.newArrayList(typeNode.fields());

    if (fields.size() != 1) {
      throw new ConversionException(String.format("Expected exactly 1 field in node: %s", jsonNode));
    }

    final String typeString = fields.get(0).getKey();
    final ValueType type = valueHelper.getPrimitiveValueType(typeString);
    final ValueGetter getter = getterMap.get().get(type);

    if (getter == null) {
      throw new ConversionException(String.format("No getter found for type: %s", type));
    }

    final JsonNode valueNode = fields.get(0).getValue();

    if (!getter.matches(valueNode)) {
      throw new ConversionException(String.format("Getter type does not match for node: %s", valueNode));
    }

    return getter.getValue(valueNode);
  }

  private Map<ValueType, ValueGetter> getTypeToGetterMap() {
    final Map<ValueType, ValueGetter> map = new EnumMap<>(ValueType.class);

    map.put(BOOL, booleanGetter);
    map.put(BYTES, bytesGetter);
    map.put(DOUBLE, doubleGetter);
    map.put(FLOAT, floatGetter);
    map.put(INT, intGetter);
    map.put(LONG, longGetter);
    map.put(STRING, stringGetter);
    map.put(TIMESTAMP, longGetter);

    return unmodifiableMap(map);
  }
}
