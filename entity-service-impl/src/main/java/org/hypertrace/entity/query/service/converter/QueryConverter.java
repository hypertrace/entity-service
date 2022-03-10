package org.hypertrace.entity.query.service.converter;

import static java.util.Collections.unmodifiableList;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.FromTypeExpression;
import org.hypertrace.core.documentstore.query.Aggregation;
import org.hypertrace.core.documentstore.query.Filter;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Pagination.PaginationBuilder;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.Query.QueryBuilder;
import org.hypertrace.core.documentstore.query.Selection;
import org.hypertrace.core.documentstore.query.Sort;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.aggregation.AggregationColumnProvider;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.OrderByExpression;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class QueryConverter implements Converter<EntityQueryRequest, Query> {
  private final Converter<List<Expression>, Selection> selectionConverter;
  private final Converter<EntityQueryRequest, Filter> filterConverter;

  private final AggregationColumnProvider aggregationColumnProvider;
  private final Converter<List<Expression>, List<FromTypeExpression>> fromClauseConverter;
  private final Converter<List<Expression>, Aggregation> groupByConverter;

  private final Converter<List<OrderByExpression>, Sort> orderByConverter;
  private final PaginationBuilder paginationBuilder = Pagination.builder();

  @Override
  public Query convert(final EntityQueryRequest request, final RequestContext requestContext)
      throws ConversionException {
    final QueryBuilder builder = Query.builder();

    setFieldIfNotEmpty(
        request.getSelectionList(), builder::setSelection, selectionConverter, requestContext);

    final Filter filter = filterConverter.convert(request, requestContext);
    builder.setFilter(filter);

    setFieldIfNotEmpty(
        getExpressionsForFromClause(request),
        builder::addFromClauses,
        fromClauseConverter,
        requestContext);

    setFieldIfNotEmpty(
        request.getGroupByList(), builder::setAggregation, groupByConverter, requestContext);
    setFieldIfNotEmpty(
        request.getOrderByList(), builder::setSort, orderByConverter, requestContext);

    if (request.getLimit() > 0 || request.getOffset() > 0) {
      final Pagination pagination =
          paginationBuilder.limit(request.getLimit()).offset(request.getOffset()).build();
      builder.setPagination(pagination);
    }

    return builder.build();
  }

  private <T, U> void setFieldIfNotEmpty(
      final List<U> list,
      final Function<T, QueryBuilder> setter,
      final Converter<List<U>, T> converter,
      final RequestContext requestContext)
      throws ConversionException {
    if (list.isEmpty()) {
      return;
    }

    final T converted = converter.convert(list, requestContext);
    setter.apply(converted);
  }

  private List<Expression> getExpressionsForFromClause(final EntityQueryRequest request)
      throws ConversionException {
    final List<Expression> list = new ArrayList<>(request.getGroupByList());

    for (Expression expression : request.getSelectionList()) {
      if (expression.hasFunction()) {
        org.hypertrace.entity.query.service.v1.Function function = expression.getFunction();
        final Expression aggregationColumn =
            aggregationColumnProvider.getAggregationColumn(function);
        list.add(aggregationColumn);
      }
    }

    return unmodifiableList(list);
  }
}
