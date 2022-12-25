package org.hypertrace.entity.query.service.converter.accessor;

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
import static org.hypertrace.entity.query.service.v1.ValueType.STRING_MAP;
import static org.hypertrace.entity.query.service.v1.ValueType.TIMESTAMP;
import static org.hypertrace.entity.query.service.v1.ValueType.VALUE_LIST;
import static org.hypertrace.entity.query.service.v1.ValueType.VALUE_MAP;

import com.google.inject.Singleton;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;

@Singleton
public class ValueOneOfAccessor extends OneOfAccessorBase<Value, ValueType> {

  @Override
  protected Map<ValueType, Function<Value, ?>> populate() {
    final Map<ValueType, Function<Value, ?>> map = new EnumMap<>(ValueType.class);

    map.put(STRING, Value::getString);
    map.put(LONG, Value::getLong);
    map.put(INT, Value::getInt);
    map.put(FLOAT, Value::getFloat);
    map.put(DOUBLE, Value::getDouble);
    map.put(BYTES, Value::getBytes);
    map.put(BOOL, Value::getBoolean);
    map.put(TIMESTAMP, Value::getTimestamp);
    map.put(STRING_ARRAY, Value::getStringArrayList);
    map.put(LONG_ARRAY, Value::getLongArrayList);
    map.put(INT_ARRAY, Value::getIntArrayList);
    map.put(FLOAT_ARRAY, Value::getFloatArrayList);
    map.put(DOUBLE_ARRAY, Value::getDoubleArrayList);
    map.put(BYTES_ARRAY, Value::getBytesArrayList);
    map.put(BOOLEAN_ARRAY, Value::getBooleanArrayList);
    map.put(STRING_MAP, Value::getStringMapMap);
    map.put(VALUE_MAP, Value::getValueMapMap);
    map.put(VALUE_LIST, Value::getValueListList);

    return unmodifiableMap(map);
  }
}
