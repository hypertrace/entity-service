package org.hypertrace.entity.query.service.converter.filter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.query.Filter;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class FilterConverter implements Converter<EntityQueryRequest, Filter> {
  private final FilterConverterFactory filterConverterFactory;
  private final ExtraFiltersApplier extraFiltersApplier;

  @Override
  public Filter convert(final EntityQueryRequest request, final RequestContext requestContext)
      throws ConversionException {
    final org.hypertrace.entity.query.service.v1.Filter filter = request.getFilter();

    final FilterTypeExpression allFilters;

    if (org.hypertrace.entity.query.service.v1.Filter.getDefaultInstance().equals(filter)) {
      allFilters = extraFiltersApplier.getExtraFilters(request, requestContext);
    } else {
      final Converter<org.hypertrace.entity.query.service.v1.Filter, ? extends FilterTypeExpression>
          filterConverter = filterConverterFactory.getFilterConverter(filter.getOperator());
      final FilterTypeExpression filterTypeExpression =
          filterConverter.convert(filter, requestContext);
      allFilters =
          extraFiltersApplier.addExtraFilters(filterTypeExpression, request, requestContext);
    }

    return Filter.builder().expression(allFilters).build();
  }
}
