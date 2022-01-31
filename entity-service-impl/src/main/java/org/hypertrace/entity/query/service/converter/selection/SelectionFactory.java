package org.hypertrace.entity.query.service.converter.selection;

import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.entity.query.service.converter.AliasProvider;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.v1.Expression;

public interface SelectionFactory {
  <T> Converter<T, ? extends SelectingExpression> getConverter(final Expression.ValueCase valueCase)
      throws ConversionException;

  <T> AliasProvider<T> getAliasProvider(final Expression.ValueCase valueCase)
      throws ConversionException;
}
