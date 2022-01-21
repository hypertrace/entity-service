package org.hypertrace.entity.query.service.converter.filter;

import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;

public interface ExtraFiltersApplier {
  LogicalExpression addExtraFilters(
      final FilteringExpression filters,
      final EntityQueryRequest entityQueryRequest,
      final RequestContext context);
}
