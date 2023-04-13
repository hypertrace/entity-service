package org.hypertrace.entity.query.service.converter.filter;

import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.LITERAL;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class RelationalExpressionConverter implements Converter<Filter, FilterTypeExpression> {
  private final OneOfAccessor<Expression, ValueCase> expressionAccessor;
  private final FilteringExpressionConverterFactory filteringExpressionConverterFactory;

  @Override
  public FilterTypeExpression convert(final Filter filter, final RequestContext requestContext)
      throws ConversionException {
    final Expression lhs = filter.getLhs();
    final Operator operator = filter.getOperator();
    final Expression rhs = filter.getRhs();

    final ColumnIdentifier identifier =
        expressionAccessor.access(lhs, lhs.getValueCase(), Set.of(COLUMNIDENTIFIER));
    final LiteralConstant literal =
        expressionAccessor.access(rhs, rhs.getValueCase(), Set.of(LITERAL));

    final FilteringExpressionConverter filteringExpressionConverter =
        filteringExpressionConverterFactory.getConverter(
            identifier.getColumnName(), literal.getValue(), operator, requestContext);
    return filteringExpressionConverter.convert(identifier, operator, literal, requestContext);
  }
}
