package org.hypertrace.entity.query.service.converter.identifier;

import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUES_KEY;
import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUE_KEY;
import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUE_LIST_KEY;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;

/** Adds suffix .valueList.values.value.&lt;type&gt; for direct comparison */
@Singleton
public class ArraySuffixAddingIdentifierConverter extends SuffixAddingIdentifierConverter {
  private static final String ARRAY_SUFFIX =
      "." + VALUE_LIST_KEY + "." + VALUES_KEY + "." + VALUE_KEY + ".";

  @Inject
  public ArraySuffixAddingIdentifierConverter(final ValueHelper valueHelper) {
    super(valueHelper);
  }

  @Override
  protected String getSuffix(final IdentifierConversionMetadata metadata)
      throws ConversionException {
    return ARRAY_SUFFIX + getTypeName(metadata);
  }
}
