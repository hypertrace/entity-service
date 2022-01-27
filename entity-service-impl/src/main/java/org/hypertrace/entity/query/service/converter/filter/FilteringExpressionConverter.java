package org.hypertrace.entity.query.service.converter.filter;

import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;

public interface FilteringExpressionConverter {

  FilteringExpression convert(
      ColumnIdentifier columnIdentifier,
      Operator operator,
      LiteralConstant constant,
      RequestContext requestContext)
      throws ConversionException;
}
