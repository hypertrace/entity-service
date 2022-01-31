package org.hypertrace.entity.query.service.converter.filter;

import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;

public interface ExtraFiltersApplier {
  FilteringExpression addExtraFilters(
      final FilteringExpression filters,
      final EntityQueryRequest entityQueryRequest,
      final RequestContext context);

  FilteringExpression getExtraFilters(
      final EntityQueryRequest entityQueryRequest, final RequestContext context);
}
