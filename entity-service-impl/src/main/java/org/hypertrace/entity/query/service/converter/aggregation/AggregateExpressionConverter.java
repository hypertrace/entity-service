package org.hypertrace.entity.query.service.converter.aggregation;

import static com.google.common.base.Suppliers.memoize;
import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.AVG;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MAX;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MIN;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.SUM;
import static org.hypertrace.entity.query.service.v1.AggregationOperator.AGGREGATION_OPERATOR_AVG;
import static org.hypertrace.entity.query.service.v1.AggregationOperator.AGGREGATION_OPERATOR_COUNT;
import static org.hypertrace.entity.query.service.v1.AggregationOperator.AGGREGATION_OPERATOR_DISTINCT;
import static org.hypertrace.entity.query.service.v1.AggregationOperator.AGGREGATION_OPERATOR_DISTINCT_COUNT;
import static org.hypertrace.entity.query.service.v1.AggregationOperator.AGGREGATION_OPERATOR_MAX;
import static org.hypertrace.entity.query.service.v1.AggregationOperator.AGGREGATION_OPERATOR_MIN;
import static org.hypertrace.entity.query.service.v1.AggregationOperator.AGGREGATION_OPERATOR_SUM;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.EnumMap;
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

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class AggregateExpressionConverter
    implements Converter<
        org.hypertrace.entity.query.service.v1.AggregateExpression, AggregateExpression> {
  private static final Supplier<
          Map<org.hypertrace.entity.query.service.v1.AggregationOperator, AggregationOperator>>
      OPERATOR_MAP = memoize(AggregateExpressionConverter::getOperatorMap);

  private final OneOfAccessor<Expression, ValueCase> expressionAccessor;
  private final Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;

  @Override
  public AggregateExpression convert(
      final org.hypertrace.entity.query.service.v1.AggregateExpression aggregateExpression,
      final RequestContext requestContext)
      throws ConversionException {
    final AggregationOperator operator = OPERATOR_MAP.get().get(aggregateExpression.getOperator());

    if (operator == null) {
      throw new ConversionException(
          String.format("Operator not found for: %s", aggregateExpression));
    }

    final Expression innerExpression = aggregateExpression.getExpression();
    final ColumnIdentifier containingIdentifier =
        expressionAccessor.access(
            innerExpression, innerExpression.getValueCase(), Set.of(COLUMNIDENTIFIER));
    final IdentifierExpression identifierExpression =
        identifierExpressionConverter.convert(containingIdentifier, requestContext);

    return AggregateExpression.of(operator, identifierExpression);
  }

  private static Map<
          org.hypertrace.entity.query.service.v1.AggregationOperator, AggregationOperator>
      getOperatorMap() {
    final Map<org.hypertrace.entity.query.service.v1.AggregationOperator, AggregationOperator> map =
        new EnumMap<>(org.hypertrace.entity.query.service.v1.AggregationOperator.class);

    map.put(AGGREGATION_OPERATOR_AVG, AVG);
    map.put(AGGREGATION_OPERATOR_MIN, MIN);
    map.put(AGGREGATION_OPERATOR_MAX, MAX);
    map.put(AGGREGATION_OPERATOR_SUM, SUM);
    map.put(AGGREGATION_OPERATOR_COUNT, COUNT);
    map.put(AGGREGATION_OPERATOR_DISTINCT_COUNT, DISTINCT_COUNT);
    map.put(AGGREGATION_OPERATOR_DISTINCT, DISTINCT);

    return unmodifiableMap(map);
  }
}
