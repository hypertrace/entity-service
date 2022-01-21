package org.hypertrace.entity.query.service.converter.filter;

import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.entity.service.constants.EntityServiceConstants.ENTITY_TYPE;
import static org.hypertrace.entity.service.constants.EntityServiceConstants.TENANT_ID;

import com.google.inject.Singleton;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;

@Singleton
public class ExtraFiltersApplierImpl implements ExtraFiltersApplier {

  @Override
  public LogicalExpression addExtraFilters(
      final FilteringExpression filters,
      final EntityQueryRequest entityQueryRequest,
      final RequestContext context) {
    final RelationalExpression tenantIdFilter = getTenantIdFilter(context);
    final RelationalExpression entityTypeFilter = getEntityTypeFilter(entityQueryRequest);

    return LogicalExpression.builder()
        .operator(AND)
        .operand(filters)
        .operand(tenantIdFilter)
        .operand(entityTypeFilter)
        .build();
  }

  @Override
  public LogicalExpression getExtraFilters(final EntityQueryRequest entityQueryRequest,
      final RequestContext context) {
    final RelationalExpression tenantIdFilter = getTenantIdFilter(context);
    final RelationalExpression entityTypeFilter = getEntityTypeFilter(entityQueryRequest);

    return LogicalExpression.builder()
        .operator(AND)
        .operand(tenantIdFilter)
        .operand(entityTypeFilter)
        .build();
  }

  private RelationalExpression getTenantIdFilter(final RequestContext context) {
    final String tenantId = context.getTenantId().orElseThrow();
    return RelationalExpression.of(
        IdentifierExpression.of(TENANT_ID), EQ, ConstantExpression.of(tenantId));
  }

  private RelationalExpression getEntityTypeFilter(final EntityQueryRequest entityQueryRequest) {
    final String entityType = entityQueryRequest.getEntityType();
    return RelationalExpression.of(
        IdentifierExpression.of(ENTITY_TYPE), EQ, ConstantExpression.of(entityType));
  }
}
