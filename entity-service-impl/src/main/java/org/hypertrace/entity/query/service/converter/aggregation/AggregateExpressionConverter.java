package org.hypertrace.entity.query.service.converter.aggregation;

import static com.google.common.base.Suppliers.memoize;
import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.AVG;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_ARRAY;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MAX;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MIN;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.SUM;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.Function;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class AggregateExpressionConverter implements Converter<Function, AggregateExpression> {
  private static final Supplier<Map<String, AggregationOperator>> OPERATOR_MAP =
      memoize(AggregateExpressionConverter::getOperatorMap);

  private final OneOfAccessor<Expression, ValueCase> expressionAccessor;
  private final Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;

  @Override
  public AggregateExpression convert(
      final Function aggregateExpression, final RequestContext requestContext)
      throws ConversionException {
    final AggregationOperator operator =
        OPERATOR_MAP.get().get(aggregateExpression.getFunctionName());

    if (operator == null) {
      throw new ConversionException(
          String.format("Operator not found for: %s", aggregateExpression));
    }

    final List<Expression> innerExpressions = aggregateExpression.getArgumentsList();

    if (innerExpressions.size() != 1) {
      throw new ConversionException("Aggregation function should have exactly one argument");
    }

    final Expression innerExpression = innerExpressions.get(0);

    final ColumnIdentifier containingIdentifier =
        expressionAccessor.access(
            innerExpression, innerExpression.getValueCase(), Set.of(COLUMNIDENTIFIER));
    final IdentifierExpression identifierExpression =
        identifierExpressionConverter.convert(containingIdentifier, requestContext);

    return AggregateExpression.of(operator, identifierExpression);
  }

  @SuppressWarnings("Java9CollectionFactory")
  private static Map<String, AggregationOperator> getOperatorMap() {
    final Map<String, AggregationOperator> map = new HashMap<>();

    map.put("AVG", AVG);
    map.put("MIN", MIN);
    map.put("MAX", MAX);
    map.put("SUM", SUM);
    map.put("COUNT", COUNT);
    map.put("DISTINCTCOUNT", DISTINCT_COUNT);
    // Note: The usage of DISTINCT is deprecated and would be removed once the upstream services are
    // migrated
    map.put("DISTINCT", DISTINCT_ARRAY);
    map.put("DISTINCT_ARRAY", DISTINCT_ARRAY);

    return unmodifiableMap(map);
  }
}
