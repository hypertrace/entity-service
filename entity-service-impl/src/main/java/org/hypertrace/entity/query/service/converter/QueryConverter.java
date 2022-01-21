package org.hypertrace.entity.query.service.converter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.query.Aggregation;
import org.hypertrace.core.documentstore.query.Filter;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Pagination.PaginationBuilder;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.Selection;
import org.hypertrace.core.documentstore.query.Sort;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.GroupByExpression;
import org.hypertrace.entity.query.service.v1.OrderByExpression;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class QueryConverter implements Converter<EntityQueryRequest, Query> {
  private final Converter<List<Expression>, Selection> selectionConverter;
  private final Converter<EntityQueryRequest, Filter> filterConverter;

  private final Converter<List<GroupByExpression>, Aggregation> groupByConverter;

  private final Converter<List<OrderByExpression>, Sort> sortConverter;
  private final PaginationBuilder paginationBuilder = Pagination.builder();

  @Override
  public Query convert(final EntityQueryRequest request, final RequestContext requestContext)
      throws ConversionException {
    final Selection selection =
        selectionConverter.convert(request.getSelectionList(), requestContext);
    final Filter filter = filterConverter.convert(request, requestContext);

    final Aggregation aggregation =
        groupByConverter.convert(request.getGroupByList(), requestContext);

    final Sort sort = sortConverter.convert(request.getOrderByList(), requestContext);
    final Pagination pagination =
        paginationBuilder.limit(request.getLimit()).offset(request.getOffset()).build();

    return Query.builder()
        .setSelection(selection)
        .setFilter(filter)
        .setAggregation(aggregation)
        .setSort(sort)
        .setPagination(pagination)
        .build();
  }
}
