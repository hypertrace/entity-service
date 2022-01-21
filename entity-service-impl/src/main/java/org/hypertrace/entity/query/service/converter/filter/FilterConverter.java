package org.hypertrace.entity.query.service.converter.filter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.documentstore.query.Filter;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class FilterConverter
    implements Converter<org.hypertrace.entity.query.service.v1.Filter, Filter> {
  private final FilterConverterFactory filterConverterFactory;

  @Override
  public Filter convert(
      final org.hypertrace.entity.query.service.v1.Filter filter,
      final RequestContext requestContext)
      throws ConversionException {
    final Converter<org.hypertrace.entity.query.service.v1.Filter, ? extends FilteringExpression>
        filterConverter = filterConverterFactory.getFilterConverter(filter.getOperator());
    return Filter.builder().expression(filterConverter.convert(filter, requestContext)).build();
  }
}
