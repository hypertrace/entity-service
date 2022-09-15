package org.hypertrace.entity.query.service.converter.filter;

import com.google.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierExpressionConverter;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class NullFilteringExpressionConverter extends FilteringExpressionConverterBase {
  private final IdentifierExpressionConverter identifierExpressionConverter;
  private final Converter<LiteralConstant, ConstantExpression> constantExpressionConverter;

  @Override
  public FilterTypeExpression convert(
      final ColumnIdentifier columnIdentifier,
      final Operator operator,
      final LiteralConstant constant,
      final RequestContext requestContext)
      throws ConversionException {
    final IdentifierExpression identifierExpression =
        identifierExpressionConverter.convert(columnIdentifier, requestContext);

    final RelationalOperator relationalOperator = convertOperator(operator);
    final ConstantExpression constantExpression =
        constantExpressionConverter.convert(constant, requestContext);

    RelationalExpression relationalExpression =
        RelationalExpression.of(identifierExpression, relationalOperator, constantExpression);

    /**
     * An array is stored as 'attributes.array_field.valueList.values.%d.value.&lt;type&gt;'.
     *
     * <p>Example: products = ['shoes', 'socks'] is stored as
     * 'attributes.names.valueList.values.0.value.string: shoes', and
     * 'attributes.names.valueList.values.1.value.string: socks'.
     *
     * <p>An empty array is stored as 'attributes.array_field.valueList: {}'
     */
    switch (operator) {
      case EQ:
        // 'field' EQ 'null' -> 'field' NOT_EXISTS
        // array 'field' EQ 'null' -> 'field.valueList.values' NOT_EXISTS
        return RelationalExpression.of(
            identifierExpression, convertOperator(Operator.NOT_EXISTS), constantExpression);

      case NEQ:
        // 'field' NEQ 'null' -> 'field' EXISTS
        // array 'field' NEQ 'null' -> 'field.valueList.values' EXISTS
        return RelationalExpression.of(
            identifierExpression, convertOperator(Operator.EXISTS), constantExpression);

      default:
        return relationalExpression;
    }
  }
}
