package org.hypertrace.entity.query.service.converter.identifier;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.hypertrace.entity.query.service.converter.ValueHelper;
import org.hypertrace.entity.query.service.v1.Operator;

@Singleton
public class ArraySuffixAddingIdentifierConverter extends SuffixAddingIdentifierConverter {
  private static final String IN_CLAUSE_SUFFIX = ".valueList.values.value.";
  private static final String OTHER_CLAUSE_SUFFIX = ".valueList.values.%d.value.";

  @Inject
  public ArraySuffixAddingIdentifierConverter(final ValueHelper valueHelper) {
    super(valueHelper);
  }

  @Override
  protected String getSuffix(final Operator operator) {
    switch (operator) {
      case IN:
      case NOT_IN:
        return IN_CLAUSE_SUFFIX;

      default:
        return OTHER_CLAUSE_SUFFIX;
    }
  }
}
