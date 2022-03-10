package org.hypertrace.entity.query.service.converter.identifier;

import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUES_KEY;
import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUE_KEY;
import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUE_LIST_KEY;
import static org.hypertrace.entity.query.service.converter.filter.FilteringExpressionConverterBase.ARRAY_OPERATORS;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;

/**
 * Adds suffix .valueList.values.%d.value.&lt;type&gt; for element-by-element comparison.
 *
 * <p>The placeholder '%d' needs to be replaced with array index by the calling method
 */
@Singleton
public class ArrayElementSuffixAddingIdentifierConverter extends SuffixAddingIdentifierConverter {
  private static final String ARRAY_ELEMENT_SUFFIX =
      "." + VALUE_LIST_KEY + "." + VALUES_KEY + ".%d." + VALUE_KEY + ".";
  private final ArraySuffixAddingIdentifierConverter arraySuffixAddingIdentifierConverter;

  @Inject
  public ArrayElementSuffixAddingIdentifierConverter(
      final ValueHelper valueHelper,
      final ArraySuffixAddingIdentifierConverter arraySuffixAddingIdentifierConverter) {
    super(valueHelper);
    this.arraySuffixAddingIdentifierConverter = arraySuffixAddingIdentifierConverter;
  }

  @Override
  protected String getSuffix(final IdentifierConversionMetadata metadata)
      throws ConversionException {
    if (ARRAY_OPERATORS.contains(metadata.getOperator())) {
      // If the operator is an array operator, fall-back to array suffix
      return arraySuffixAddingIdentifierConverter.getSuffix(metadata);
    }

    return ARRAY_ELEMENT_SUFFIX + getTypeName(metadata);
  }
}
