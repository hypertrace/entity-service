package org.hypertrace.entity.query.service.converter.identifier;

import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;
import org.hypertrace.entity.query.service.v1.Operator;

@AllArgsConstructor
public abstract class SuffixAddingIdentifierConverter extends IdentifierConverter {
  private final ValueHelper valueHelper;

  @Override
  public String convert(
      final IdentifierConversionMetadata metadata, final RequestContext requestContext)
      throws ConversionException {
    final String subDocPath = metadata.getSubDocPath();
    final String suffix = getSuffix(metadata.getOperator());
    final String typeSuffix = valueHelper.getStringValue(metadata.getValueType());

    return subDocPath + suffix + typeSuffix;
  }

  protected abstract String getSuffix(final Operator operator);
}
