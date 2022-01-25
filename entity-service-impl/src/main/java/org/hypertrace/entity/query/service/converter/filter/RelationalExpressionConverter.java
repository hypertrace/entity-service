package org.hypertrace.entity.query.service.converter.filter;

import static com.google.common.base.Suppliers.memoize;
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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilteringExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.converter.ValueHelper;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.service.constants.EntityConstants;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class RelationalExpressionConverter implements Converter<Filter, FilteringExpression> {
  private static final Supplier<Map<Operator, RelationalOperator>> RELATIONAL_OPERATOR_MAP =
      memoize(RelationalExpressionConverter::getRelationalOperatorMap);

  private final OneOfAccessor<Expression, ValueCase> expressionAccessor;
  private final Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;
  private final Converter<LiteralConstant, ConstantExpression> constantExpressionConverter;
  private final ValueHelper valueHelper;

  @Override
  public FilteringExpression convert(final Filter filter, final RequestContext requestContext)
      throws ConversionException {
    final Expression lhs = filter.getLhs();
    final Operator operator = filter.getOperator();
    final Expression rhs = filter.getRhs();

    final ColumnIdentifier identifier =
        expressionAccessor.access(lhs, lhs.getValueCase(), Set.of(COLUMNIDENTIFIER));
    final LiteralConstant literal =
        expressionAccessor.access(rhs, rhs.getValueCase(), Set.of(LITERAL));

    final IdentifierExpression identifierExpression =
        identifierExpressionConverter.convert(identifier, requestContext);

    final RelationalOperator relationalOperator = RELATIONAL_OPERATOR_MAP.get().get(operator);

    if (relationalOperator == null) {
      throw new ConversionException(
          String.format("No equivalent relational operator found for %s", operator));
    }

    if (isPartOfAttributeMap(identifierExpression.getName())) {
      return convertAttributeFilter(
          identifierExpression, relationalOperator, literal, requestContext);
    }

    return convertDirectly(identifierExpression, relationalOperator, literal, requestContext);
  }

  private RelationalExpression convertDirectly(
      final IdentifierExpression identifierExpression,
      final RelationalOperator relationalOperator,
      final LiteralConstant literal,
      final RequestContext requestContext)
      throws ConversionException {
    final ConstantExpression constantExpression =
        constantExpressionConverter.convert(literal, requestContext);

    return RelationalExpression.of(identifierExpression, relationalOperator, constantExpression);
  }

  private FilteringExpression convertAttributeFilter(
      final IdentifierExpression identifierExpression,
      final RelationalOperator relationalOperator,
      final LiteralConstant literal,
      final RequestContext requestContext)
      throws ConversionException {
    final ValueType valueType = literal.getValue().getValueType();
    final String fieldSuffix = valueHelper.getFieldSuffix(valueType);
    final String fieldName = identifierExpression.getName() + fieldSuffix;

    if (valueHelper.isMap(valueType)) {
      return convertStringMap(fieldName, relationalOperator, literal);
    }

    if (valueHelper.isList(valueType)) {
      return convertList(fieldName, relationalOperator, literal);
    }

    final IdentifierExpression newIdentifierExpression = IdentifierExpression.of(fieldName);
    return convertDirectly(newIdentifierExpression, relationalOperator, literal, requestContext);
  }

  private <T> FilteringExpression convertList(
      final String fieldName,
      final RelationalOperator relationalOperator,
      final LiteralConstant literal)
      throws ConversionException {
    final Value value = literal.getValue();
    final List<T> list = valueHelper.getList(value);

    if (list.isEmpty()) {
      throw new ConversionException("Conversion of empty-map is unsupported");
    }

    final List<RelationalExpression> expressions = new ArrayList<>();

    for (int i = 0; i < list.size(); i++) {
      final IdentifierExpression lhs = IdentifierExpression.of(String.format(fieldName, i));
      final ConstantExpression rhs = valueHelper.convertToConstantExpression(value, i);

      final RelationalExpression expression = RelationalExpression.of(lhs, relationalOperator, rhs);

      expressions.add(expression);
    }

    if (expressions.size() == 1) {
      return expressions.get(0);
    }

    return LogicalExpression.builder().operator(LogicalOperator.AND).operands(expressions).build();
  }

  private FilteringExpression convertStringMap(
      final String fieldName,
      final RelationalOperator relationalOperator,
      final LiteralConstant literal)
      throws ConversionException {
    if (relationalOperator != RelationalOperator.EQ) {
      throw new ConversionException("Non-equality comparison of String Map is unsupported");
    }

    final Map<?, ?> map = valueHelper.getMap(literal.getValue());

    if (map.isEmpty()) {
      throw new ConversionException("Conversion of empty-map is unsupported");
    }

    final List<RelationalExpression> expressions = new ArrayList<>();

    for (final Map.Entry<?, ?> entry : map.entrySet()) {
      final IdentifierExpression lhs =
          IdentifierExpression.of(String.format(fieldName, entry.getKey()));
      final ConstantExpression rhs = ConstantExpression.of(entry.getValue().toString());
      final RelationalExpression expression =
          RelationalExpression.of(lhs, RelationalOperator.EQ, rhs);

      expressions.add(expression);
    }

    if (expressions.size() == 1) {
      return expressions.get(0);
    }

    return LogicalExpression.builder().operator(LogicalOperator.AND).operands(expressions).build();
  }

  private static boolean isPartOfAttributeMap(final String fieldName) {
    return fieldName.startsWith(EntityConstants.ATTRIBUTES_MAP_PATH);
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
