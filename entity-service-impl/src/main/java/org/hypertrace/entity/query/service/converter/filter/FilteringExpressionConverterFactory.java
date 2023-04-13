package org.hypertrace.entity.query.service.converter.filter;

import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.Value;

public interface FilteringExpressionConverterFactory {
  FilteringExpressionConverter getConverter(
      final String columnName,
      final Value value,
      final Operator operator,
      final RequestContext context)
      throws ConversionException;
}
