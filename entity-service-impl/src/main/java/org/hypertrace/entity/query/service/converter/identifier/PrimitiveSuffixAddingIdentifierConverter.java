package org.hypertrace.entity.query.service.converter.identifier;

import com.google.inject.Singleton;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.ValueType;

@Singleton
public class PrimitiveSuffixAddingIdentifierConverter extends SuffixAddingIdentifierConverter {
  private static final String SUFFIX = ".value.";

  @Override
  protected String getSuffix(final ValueType valueType, final Operator operator) {
    return SUFFIX + valueType.name().toLowerCase();
  }
}
