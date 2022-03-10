package org.hypertrace.entity.query.service.converter.aggregation;

import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;

import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.Function;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class AggregationColumnProvider {
  private final OneOfAccessor<Expression, ValueCase> expressionAccessor;

  public final ColumnIdentifier getColumnIdentifier(final Function function)
      throws ConversionException {
    final List<Expression> innerExpressions = function.getArgumentsList();

    if (innerExpressions.size() != 1) {
      throw new ConversionException("Aggregation function should have exactly one argument");
    }

    final Expression innerExpression = innerExpressions.get(0);

    return expressionAccessor.access(
        innerExpression, innerExpression.getValueCase(), Set.of(COLUMNIDENTIFIER));
  }
}
