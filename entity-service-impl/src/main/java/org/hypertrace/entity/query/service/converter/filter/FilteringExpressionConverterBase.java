package org.hypertrace.entity.query.service.converter.filter;

import static com.google.common.base.Suppliers.memoize;
import static java.util.Collections.unmodifiableMap;
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

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.Operator;

public abstract class FilteringExpressionConverterBase implements FilteringExpressionConverter {
  public static final Set<Operator> ARRAY_OPERATORS = Set.of(IN, NOT_IN);

  private static final Supplier<Map<Operator, RelationalOperator>> RELATIONAL_OPERATOR_MAP =
      memoize(FilteringExpressionConverterBase::getRelationalOperatorMap);

  protected final RelationalOperator convertOperator(final Operator operator)
      throws ConversionException {
    final RelationalOperator relationalOperator = RELATIONAL_OPERATOR_MAP.get().get(operator);

    if (relationalOperator == null) {
      throw new ConversionException(
          String.format("No equivalent relational operator found for %s", operator));
    }

    return relationalOperator;
  }

  private static Map<Operator, RelationalOperator> getRelationalOperatorMap() {
    final Map<Operator, RelationalOperator> map = new EnumMap<>(Operator.class);

    // TODO: Add support for NOT
    //  map.put(NOT, RelationalOperator.NOT);
    map.put(EQ, RelationalOperator.EQ);
    map.put(NEQ, RelationalOperator.NEQ);
    map.put(IN, RelationalOperator.IN);
    map.put(NOT_IN, RelationalOperator.NOT_IN);
    // TODO: Add support for range
    //  map.put(RANGE, RelationalOperator.RANGE);
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
