package org.hypertrace.entity.query.service.converter.filter;

import static org.hypertrace.entity.query.service.converter.identifier.IdentifierConverter.getSubDocPathById;
import static org.hypertrace.entity.query.service.v1.Operator.EQ;
import static org.hypertrace.entity.query.service.v1.Operator.NEQ;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.common.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierConversionMetadata;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierConverter;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierConverterFactory;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class MapFilteringExpressionConverter extends FilteringExpressionConverterBase {
  private static final Set<Operator> SUPPORTED_OPERATORS = Set.of(EQ, NEQ);

  private final EntityAttributeMapping entityAttributeMapping;
  private final IdentifierConverterFactory identifierConverterFactory;
  private final OneOfAccessor<Value, ValueType> valueOneOfAccessor;
  private final ValueHelper valueHelper;

  @Override
  public FilterTypeExpression convert(
      final ColumnIdentifier columnIdentifier,
      final Operator operator,
      final LiteralConstant constant,
      final RequestContext requestContext)
      throws ConversionException {
    if (!SUPPORTED_OPERATORS.contains(operator)) {
      throw new ConversionException(String.format("Operator %s is not supported", operator));
    }

    final String id = columnIdentifier.getColumnName();
    final String subDocPath = getSubDocPathById(entityAttributeMapping, id, requestContext);
    final Value value = constant.getValue();
    final ValueType valueType = value.getValueType();

    final Map<?, ?> map = valueOneOfAccessor.access(value, valueType);

    if (map.isEmpty()) {
      throw new ConversionException("Conversion of empty-map is unsupported");
    }

    final IdentifierConverter identifierConverter =
        identifierConverterFactory.getIdentifierConverter(
            id, subDocPath, valueType, requestContext);

    final IdentifierConversionMetadata metadata =
        IdentifierConversionMetadata.builder()
            .subDocPath(subDocPath)
            .operator(operator)
            .valueType(valueType)
            .build();
    final String suffixedSubDocPath = identifierConverter.convert(metadata, requestContext);

    final List<RelationalExpression> expressions = new ArrayList<>();

    for (final Map.Entry<?, ?> entry : map.entrySet()) {
      final IdentifierExpression lhs =
          IdentifierExpression.of(String.format(suffixedSubDocPath, entry.getKey()));
      final RelationalOperator relationalOperator = convertOperator(operator);
      final ConstantExpression rhs = valueHelper.convertToConstantExpression(value, entry.getKey());

      final RelationalExpression expression = RelationalExpression.of(lhs, relationalOperator, rhs);

      expressions.add(expression);
    }

    if (expressions.size() == 1) {
      return expressions.get(0);
    }

    return LogicalExpression.builder().operator(LogicalOperator.AND).operands(expressions).build();
  }
}
