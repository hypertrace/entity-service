package org.hypertrace.entity.query.service.converter.accessor;

import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.FUNCTION;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.LITERAL;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.ORDERBY;

import com.google.inject.Singleton;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;

@Singleton
public class ExpressionOneOfAccessor extends OneOfAccessorBase<Expression, ValueCase> {

  @Override
  protected Map<ValueCase, Function<Expression, ?>> populate() {
    final Map<ValueCase, Function<Expression, ?>> map = new EnumMap<>(ValueCase.class);

    map.put(COLUMNIDENTIFIER, Expression::getColumnIdentifier);
    map.put(LITERAL, Expression::getLiteral);
    map.put(FUNCTION, Expression::getFunction);
    map.put(ORDERBY, Expression::getOrderBy);

    return unmodifiableMap(map);
  }
}
