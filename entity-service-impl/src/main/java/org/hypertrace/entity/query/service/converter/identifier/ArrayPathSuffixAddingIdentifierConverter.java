package org.hypertrace.entity.query.service.converter.identifier;

import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUES_KEY;
import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUE_LIST_KEY;

import javax.inject.Inject;
import javax.inject.Singleton;
import org.hypertrace.entity.query.service.converter.ValueHelper;

/** Adds suffix .valueList.values for unwinding */
@Singleton
public class ArrayPathSuffixAddingIdentifierConverter extends SuffixAddingIdentifierConverter {
  private static final String ARRAY_PATH_SUFFIX = "." + VALUE_LIST_KEY + "." + VALUES_KEY;

  @Inject
  public ArrayPathSuffixAddingIdentifierConverter(final ValueHelper valueHelper) {
    super(valueHelper);
  }

  @Override
  protected String getSuffix(final IdentifierConversionMetadata metadata) {
    return ARRAY_PATH_SUFFIX;
  }
}
