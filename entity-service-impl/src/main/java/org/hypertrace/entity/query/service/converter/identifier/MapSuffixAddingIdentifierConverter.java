package org.hypertrace.entity.query.service.converter.identifier;

import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUES_KEY;
import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUE_KEY;
import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUE_MAP_KEY;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;

/**
 * Adds suffix .valueMap.values.%s.value.&lt;type&gt; for element-by-element comparison.
 *
 * <p>The placeholder '%s' needs to be replaced with map key by the calling method
 */
@Singleton
public class MapSuffixAddingIdentifierConverter extends SuffixAddingIdentifierConverter {
  private static final String SUFFIX =
      "." + VALUE_MAP_KEY + "." + VALUES_KEY + ".%s." + VALUE_KEY + ".";

  @Inject
  public MapSuffixAddingIdentifierConverter(final ValueHelper valueHelper) {
    super(valueHelper);
  }

  @Override
  protected String getSuffix(final IdentifierConversionMetadata metadata)
      throws ConversionException {
    return SUFFIX + getTypeName(metadata);
  }
}
