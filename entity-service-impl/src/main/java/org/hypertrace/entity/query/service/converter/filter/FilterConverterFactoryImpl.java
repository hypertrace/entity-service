package org.hypertrace.entity.query.service.converter.filter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.Operator;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class FilterConverterFactoryImpl implements FilterConverterFactory {
  private final RelationalExpressionConverter relationalExpressionConverter;
  private final LogicalExpressionConverter logicalExpressionConverter;

  @Override
  public Converter<Filter, FilteringExpression> getFilterConverter(final Operator operator)
      throws ConversionException {
    switch (operator) {
      case AND:
      case OR:
        return logicalExpressionConverter;

      case UNRECOGNIZED:
        throw new ConversionException(
            String.format("No converter found for operator: %s", operator));

      default:
        return relationalExpressionConverter;
    }
  }
}
