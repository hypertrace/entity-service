package org.hypertrace.entity.query.service.converter.identifier;

import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;

@AllArgsConstructor
public abstract class SuffixAddingIdentifierConverter extends IdentifierConverter {
  private final ValueHelper valueHelper;

  @Override
  public String convert(
      final IdentifierConversionMetadata metadata, final RequestContext requestContext)
      throws ConversionException {
    final String subDocPath = metadata.getSubDocPath();
    final String suffix = getSuffix(metadata);

    return subDocPath + suffix;
  }

  protected final String getTypeName(final IdentifierConversionMetadata metadata)
      throws ConversionException {
    return valueHelper.getStringValue(metadata.getValueType());
  }

  protected abstract String getSuffix(final IdentifierConversionMetadata metadata)
      throws ConversionException;
}
