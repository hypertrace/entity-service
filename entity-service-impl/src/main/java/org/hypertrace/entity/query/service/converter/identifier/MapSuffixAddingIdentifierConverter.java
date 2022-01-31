package org.hypertrace.entity.query.service.converter.identifier;

import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUE_MAP_KEY;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.hypertrace.entity.query.service.converter.ValueHelper;
import org.hypertrace.entity.query.service.v1.Operator;

@Singleton
public class MapSuffixAddingIdentifierConverter extends SuffixAddingIdentifierConverter {
  private static final String SUFFIX = "." + VALUE_MAP_KEY + ".values.%s.value.";

  @Inject
  public MapSuffixAddingIdentifierConverter(final ValueHelper valueHelper) {
    super(valueHelper);
  }

  @Override
  protected String getSuffix(final Operator operator) {
    return SUFFIX;
  }
}
