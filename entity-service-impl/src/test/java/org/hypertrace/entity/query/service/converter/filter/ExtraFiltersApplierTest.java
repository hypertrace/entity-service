package org.hypertrace.entity.query.service.converter.filter;

import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.entity.service.constants.EntityServiceConstants.ENTITY_TYPE;
import static org.hypertrace.entity.service.constants.EntityServiceConstants.TENANT_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.quality.Strictness.LENIENT;

import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
public class ExtraFiltersApplierTest {
  private final ExtraFiltersApplier extraFiltersApplier = new ExtraFiltersApplierImpl();

  @Mock private FilteringExpression filter;

  private final String tenantId = "Mars-man";
  private final String entityType = "SERVICE";

  private final EntityQueryRequest request =
      EntityQueryRequest.newBuilder().setEntityType(entityType).build();
  private final RequestContext context = RequestContext.forTenantId(tenantId);

  private final RelationalExpression entityTypeFilter =
      RelationalExpression.of(
          IdentifierExpression.of(ENTITY_TYPE), EQ, ConstantExpression.of(entityType));
  private final RelationalExpression tenantIdFilter =
      RelationalExpression.of(
          IdentifierExpression.of(TENANT_ID), EQ, ConstantExpression.of(tenantId));

  @Test
  void testGetExtraFilters() {
    final LogicalExpression expected =
        LogicalExpression.builder()
            .operator(AND)
            .operand(tenantIdFilter)
            .operand(entityTypeFilter)
            .build();
    assertEquals(expected, extraFiltersApplier.getExtraFilters(request, context));
  }

  @Test
  void testAddExtraFilters() {
    final LogicalExpression expected =
        LogicalExpression.builder()
            .operator(AND)
            .operand(filter)
            .operand(tenantIdFilter)
            .operand(entityTypeFilter)
            .build();
    assertEquals(expected, extraFiltersApplier.addExtraFilters(filter, request, context));
  }
}
