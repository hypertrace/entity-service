package org.hypertrace.entity.query.service.converter.identifier;

import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.ValueType;

public abstract class SuffixAddingIdentifierConverter extends IdentifierConverter {
  @Override
  public String convert(
      final IdentifierConversionMetadata metadata, final RequestContext requestContext)
      throws ConversionException {
    final String subDocPath = metadata.getSubDocPath();
    final String suffix = getSuffix(metadata.getValueType(), metadata.getOperator());

    return subDocPath + suffix;
  }

  protected abstract String getSuffix(final ValueType valueType, final Operator operator);
}
