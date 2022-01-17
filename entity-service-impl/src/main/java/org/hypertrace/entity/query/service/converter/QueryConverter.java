package org.hypertrace.entity.query.service.converter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.query.Filter;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Pagination.PaginationBuilder;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.Selection;
import org.hypertrace.core.documentstore.query.Sort;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.OrderByExpression;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class QueryConverter implements Converter<EntityQueryRequest, Query> {
  private final Converter<List<Expression>, Selection> selectionConverter;
  private final Converter<org.hypertrace.entity.query.service.v1.Filter, Filter> filterConverter;

  private final Converter<List<OrderByExpression>, Sort> sortConverter;
  private final PaginationBuilder paginationBuilder = Pagination.builder();

  @Override
  public Query convert(final EntityQueryRequest request) throws ConversionException {
    final Selection selection = selectionConverter.convert(request.getSelectionList());
    final Filter filter = filterConverter.convert(request.getFilter());
    final Sort sort = sortConverter.convert(request.getOrderByList());
    final Pagination pagination = paginationBuilder.limit(request.getLimit()).offset(request.getOffset()).build();

    return Query.builder().setSelection(selection).setFilter(filter).setSort(sort).setPagination(pagination).build();
  }
}
