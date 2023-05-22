package org.hypertrace.entity.query.service.converter.filter;

import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.Value;

public interface FilteringExpressionConverterFactory {
  FilteringExpressionConverter getConverter(
      final ColumnIdentifier identifier,
      final Operator operator,
      final Value value,
      final RequestContext context)
      throws ConversionException;
}
