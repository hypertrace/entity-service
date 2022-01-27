package org.hypertrace.entity.query.service.converter.identifier;

import static com.google.common.base.Suppliers.memoize;
import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.entity.query.service.v1.ValueType.BOOL;
import static org.hypertrace.entity.query.service.v1.ValueType.BOOLEAN_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.BYTES;
import static org.hypertrace.entity.query.service.v1.ValueType.BYTES_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.DOUBLE;
import static org.hypertrace.entity.query.service.v1.ValueType.DOUBLE_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.FLOAT;
import static org.hypertrace.entity.query.service.v1.ValueType.FLOAT_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.INT;
import static org.hypertrace.entity.query.service.v1.ValueType.INT_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.LONG;
import static org.hypertrace.entity.query.service.v1.ValueType.LONG_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING_ARRAY;

import com.google.inject.Singleton;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Supplier;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.ValueType;

@Singleton
public class ArraySuffixAddingIdentifierConverter extends SuffixAddingIdentifierConverter {
  private static final String IN_CLAUSE_SUFFIX = ".valueList.values.value.";
  private static final String OTHER_CLAUSE_SUFFIX = ".valueList.values.%d.value.";
  private static final Supplier<Map<ValueType, ValueType>> ARRAY_TYPE_TO_PRIMITIVE_TYPE_MAP =
      memoize(ArraySuffixAddingIdentifierConverter::getArrayTypeToPrimitiveTypeMap);

  @Override
  protected String getSuffix(final ValueType valueType, final Operator operator) {
    final ValueType primitiveType = ARRAY_TYPE_TO_PRIMITIVE_TYPE_MAP.get().get(valueType);
    return getSuffix(operator) + primitiveType.name().toLowerCase();
  }

  protected String getSuffix(final Operator operator) {
    switch (operator) {
      case IN:
      case NOT_IN:
        return IN_CLAUSE_SUFFIX;

      default:
        return OTHER_CLAUSE_SUFFIX;
    }
  }

  private static Map<ValueType, ValueType> getArrayTypeToPrimitiveTypeMap() {
    final Map<ValueType, ValueType> map = new EnumMap<>(ValueType.class);

    map.put(STRING_ARRAY, STRING);
    map.put(LONG_ARRAY, LONG);
    map.put(INT_ARRAY, INT);
    map.put(FLOAT_ARRAY, FLOAT);
    map.put(DOUBLE_ARRAY, DOUBLE);
    map.put(BYTES_ARRAY, BYTES);
    map.put(BOOLEAN_ARRAY, BOOL);

    return unmodifiableMap(map);
  }
}
