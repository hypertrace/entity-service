package org.hypertrace.entity.query.service.converter.identifier;

import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUE_KEY;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;

/** Adds suffix value.&lt;type&gt; for direct comparison. */
@Singleton
public class PrimitiveSuffixAddingIdentifierConverter extends SuffixAddingIdentifierConverter {
  private static final String SUFFIX = "." + VALUE_KEY + ".";

  @Inject
  public PrimitiveSuffixAddingIdentifierConverter(final ValueHelper valueHelper) {
    super(valueHelper);
  }

  @Override
  protected String getSuffix(final IdentifierConversionMetadata metadata)
      throws ConversionException {
    return SUFFIX + getTypeName(metadata);
  }
}
