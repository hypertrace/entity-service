package org.hypertrace.entity.query.service.converter.filter;

import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;

public interface FilteringExpressionConverter {

  FilterTypeExpression convert(
      ColumnIdentifier columnIdentifier,
      Operator operator,
      LiteralConstant constant,
      RequestContext requestContext)
      throws ConversionException;
}
