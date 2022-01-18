package org.hypertrace.entity.query.service.converter.filter;

import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.Operator;

public interface FilterConverterFactory {
  Converter<Filter, ? extends FilteringExpression> getFilterConverter(final Operator operator)
      throws ConversionException;
}
