package org.hypertrace.entity.query.service.converter.filter;

import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.ValueType;

public interface FilteringExpressionConverterFactory {
  FilteringExpressionConverter getConverter(final ValueType valueType) throws ConversionException;
}
