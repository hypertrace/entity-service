package org.hypertrace.entity.query.service.converter.aggregation;

import java.util.List;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Function;

public class AggregationColumnProvider {

  public final Expression getAggregationColumn(final Function function) throws ConversionException {
    final List<Expression> innerExpressions = function.getArgumentsList();

    if (innerExpressions.size() != 1) {
      throw new ConversionException("Aggregation function should have exactly one argument");
    }

    return innerExpressions.get(0);
  }
}
