package org.hypertrace.entity.query.service.converter.identifier;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.hypertrace.entity.query.service.converter.ValueHelper;
import org.hypertrace.entity.query.service.v1.Operator;

/** Adds suffix value.&lt;type&gt; for direct comparison. */
@Singleton
public class PrimitiveSuffixAddingIdentifierConverter extends SuffixAddingIdentifierConverter {
  private static final String SUFFIX = ".value.";

  @Inject
  public PrimitiveSuffixAddingIdentifierConverter(final ValueHelper valueHelper) {
    super(valueHelper);
  }

  @Override
  protected String getSuffix(final Operator operator) {
    return SUFFIX;
  }
}
