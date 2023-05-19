package org.hypertrace.entity.query.service.converter.filter;

import static java.util.Map.entry;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.OR;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.CONTAINS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_CONTAINS;
import static org.hypertrace.entity.query.service.converter.identifier.IdentifierConverter.getSubDocPathById;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.experimental.Accessors;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;
import org.hypertrace.entity.query.service.converter.identifier.ArrayPathSuffixAddingIdentifierConverter;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierConversionMetadata;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class ContainsFilteringExpressionConverter extends FilteringExpressionConverterBase {
  private static final Map<Operator, OperatorPair> OPERATOR_MAP = Map.ofEntries(
      entry(Operator.IN, OperatorPair.of(CONTAINS, OR)),
      entry(Operator.NOT_IN, OperatorPair.of(NOT_CONTAINS, AND))
  );

  private final EntityAttributeMapping entityAttributeMapping;
  private final ArrayPathSuffixAddingIdentifierConverter arrayPathSuffixAddingIdentifierConverter;
  private final ValueHelper valueHelper;

  @Override
  public FilterTypeExpression convert(
      final ColumnIdentifier columnIdentifier,
      final Operator operator,
      final LiteralConstant constant,
      final RequestContext requestContext)
      throws ConversionException {
    final String id = columnIdentifier.getColumnName();
    final String subDocPath = getSubDocPathById(entityAttributeMapping, id, requestContext);
    final Value value = constant.getValue();
    final ValueType valueType = value.getValueType();

    final List<Document> list = valueHelper.getDocumentListFromArrayValue(value);

    if (list.isEmpty()) {
      throw new ConversionException("Conversion of empty-list is unsupported");
    }

    final OperatorPair operatorPair = OPERATOR_MAP.get(operator);
    if (operatorPair == null) {
      throw new ConversionException(
          String.format("Operator %s is not supported on array column %s", operator, id));
    }

    final IdentifierConversionMetadata metadata =
        IdentifierConversionMetadata.builder()
            .subDocPath(subDocPath)
            .operator(operator)
            .valueType(valueType)
            .build();
    final String suffixedSubDocPath = arrayPathSuffixAddingIdentifierConverter.convert(metadata, requestContext);
    final IdentifierExpression lhs = IdentifierExpression.of(suffixedSubDocPath);

    final List<RelationalExpression> expressions = new ArrayList<>();

    for (final Document document : list) {
      final ConstantExpression rhs = ConstantExpression.of(document);
      final RelationalExpression expression = RelationalExpression.of(lhs,
          operatorPair.relationalOperator(), rhs);

      expressions.add(expression);
    }

    if (expressions.size() == 1) {
      return expressions.get(0);
    }

    return LogicalExpression.builder().operator(operatorPair.logicalOperator()).operands(expressions).build();
  }

  @lombok.Value
  @Accessors(fluent = true)
  @AllArgsConstructor(staticName = "of")
  private static class OperatorPair {
    RelationalOperator relationalOperator;
    LogicalOperator logicalOperator;
  }
}
