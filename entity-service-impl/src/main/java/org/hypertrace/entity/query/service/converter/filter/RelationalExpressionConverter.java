package org.hypertrace.entity.query.service.converter.filter;

import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.LITERAL;
import static org.hypertrace.entity.query.service.v1.Operator.EQ;
import static org.hypertrace.entity.query.service.v1.Operator.EXISTS;
import static org.hypertrace.entity.query.service.v1.Operator.GE;
import static org.hypertrace.entity.query.service.v1.Operator.GT;
import static org.hypertrace.entity.query.service.v1.Operator.IN;
import static org.hypertrace.entity.query.service.v1.Operator.LE;
import static org.hypertrace.entity.query.service.v1.Operator.LIKE;
import static org.hypertrace.entity.query.service.v1.Operator.LT;
import static org.hypertrace.entity.query.service.v1.Operator.NEQ;
import static org.hypertrace.entity.query.service.v1.Operator.NOT_EXISTS;
import static org.hypertrace.entity.query.service.v1.Operator.NOT_IN;

import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.converter.accessor.IOneOfAccessor;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class RelationalExpressionConverter implements Converter<Filter, RelationalExpression> {
  private static final Supplier<Map<Operator, RelationalOperator>> RELATIONAL_OPERATOR_MAP =
      Suppliers.memoize(RelationalExpressionConverter::getRelationalOperatorMap);
  private final IOneOfAccessor<Expression, ValueCase> expressionAccessor;
  private final Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;
  private final Converter<LiteralConstant, ConstantExpression> constantExpressionConverter;

  @Override
  public RelationalExpression convert(final Filter filter) throws ConversionException {
    final Expression lhs = filter.getLhs();
    final Operator operator = filter.getOperator();
    final Expression rhs = filter.getRhs();

    final ColumnIdentifier identifier =
        expressionAccessor.access(lhs, lhs.getValueCase(), Set.of(COLUMNIDENTIFIER));
    final LiteralConstant literal =
        expressionAccessor.access(rhs, rhs.getValueCase(), Set.of(LITERAL));

    final IdentifierExpression identifierExpression =
        identifierExpressionConverter.convert(identifier);
    final ConstantExpression constantExpression = constantExpressionConverter.convert(literal);

    final RelationalOperator relationalOperator = RELATIONAL_OPERATOR_MAP.get().get(operator);

    if (relationalOperator == null) {
      throw new ConversionException(
          String.format("No equivalent relational operator found for %s", operator));
    }

    return RelationalExpression.of(identifierExpression, relationalOperator, constantExpression);
  }

  private static Map<Operator, RelationalOperator> getRelationalOperatorMap() {
    final Map<Operator, RelationalOperator> map = new EnumMap<>(Operator.class);

    // TODO: Add support for NOT
    //        map.put(NOT, RelationalOperator.NOT);
    map.put(EQ, RelationalOperator.EQ);
    map.put(NEQ, RelationalOperator.NEQ);
    map.put(IN, RelationalOperator.IN);
    map.put(NOT_IN, RelationalOperator.NOT_IN);
    // TODO: Add support for range
    //        map.put(RANGE, RelationalOperator.RANGE);
    map.put(GT, RelationalOperator.GT);
    map.put(LT, RelationalOperator.LT);
    map.put(GE, RelationalOperator.GTE);
    map.put(LE, RelationalOperator.LTE);
    map.put(LIKE, RelationalOperator.LIKE);
    map.put(EXISTS, RelationalOperator.EXISTS);
    map.put(NOT_EXISTS, RelationalOperator.NOT_EXISTS);

    return unmodifiableMap(map);
  }
}
