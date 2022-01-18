package org.hypertrace.entity.query.service.converter.accessor;

import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.AGGREGATION;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.FUNCTION;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.LITERAL;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.ORDERBY;

import com.google.inject.Singleton;
import org.hypertrace.entity.query.service.v1.Expression;

@Singleton
public class ExpressionOneOfAccessor extends OneOfAccessor<Expression, Expression.ValueCase> {

  @Override
  protected void populate() {
    put(COLUMNIDENTIFIER, Expression::getColumnIdentifier);
    put(LITERAL, Expression::getLiteral);
    put(FUNCTION, Expression::getFunction);
    put(ORDERBY, Expression::getOrderBy);
    put(AGGREGATION, Expression::getAggregation);
  }
}
