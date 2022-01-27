package org.hypertrace.entity.query.service.converter.identifier;

import com.google.inject.Singleton;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.ValueType;

@Singleton
public class MapSuffixAddingIdentifierConverter extends SuffixAddingIdentifierConverter {
  // Assuming the map key is always string
  private static final String SUFFIX = ".valueMap.values.%s.value.string";

  @Override
  protected String getSuffix(final ValueType valueType, final Operator operator) {
    return SUFFIX;
  }
}
