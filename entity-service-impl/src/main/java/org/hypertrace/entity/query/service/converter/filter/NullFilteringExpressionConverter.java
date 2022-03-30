package org.hypertrace.entity.query.service.converter.filter;

import static org.hypertrace.entity.query.service.converter.identifier.IdentifierConverter.getSubDocPathById;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
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
public class NullFilteringExpressionConverter extends FilteringExpressionConverterBase {
  private final EntityAttributeMapping entityAttributeMapping;
  private final IdentifierConverterFactory identifierConverterFactory;
  private final Converter<LiteralConstant, ConstantExpression> constantExpressionConverter;

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

    final IdentifierExpression identifierExpression = IdentifierExpression.of(suffixedSubDocPath);
    final RelationalOperator relationalOperator = convertOperator(operator);
    final ConstantExpression constantExpression =
        constantExpressionConverter.convert(constant, requestContext);

    RelationalExpression relationalExpression =
        RelationalExpression.of(identifierExpression, relationalOperator, constantExpression);

    // 'field' EQ 'null' -> 'field' EQ 'null' || 'field' NOT_EXISTS
    // 'field' NEQ 'null' -> 'field' NEQ 'null' || 'field' EXISTS
    Operator existenceOperator;
    switch (operator) {
      case EQ:
        existenceOperator = Operator.NOT_EXISTS;
        break;
      case NEQ:
        existenceOperator = Operator.EXISTS;
        break;
      default:
        throw new ConversionException(
            String.format("Operator %s is not supported for null value", operator));
    }

    RelationalExpression existenceRelationalExpression =
        RelationalExpression.of(
            identifierExpression, convertOperator(existenceOperator), constantExpression);

    return LogicalExpression.builder()
        .operator(LogicalOperator.OR)
        .operands(List.of(existenceRelationalExpression, relationalExpression))
        .build();
  }
}
