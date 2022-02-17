package org.hypertrace.entity.query.service.converter.filter;

import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;

public interface ExtraFiltersApplier {
  FilterTypeExpression addExtraFilters(
      final FilterTypeExpression filters,
      final EntityQueryRequest entityQueryRequest,
      final RequestContext context);

  FilterTypeExpression getExtraFilters(
      final EntityQueryRequest entityQueryRequest, final RequestContext context);
}
