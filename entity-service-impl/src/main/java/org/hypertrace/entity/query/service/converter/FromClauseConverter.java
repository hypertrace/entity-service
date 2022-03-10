package org.hypertrace.entity.query.service.converter;

import static java.util.Optional.empty;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.FUNCTION;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.expression.type.FromTypeExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.converter.aggregation.AggregationColumnProvider;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.Function;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class FromClauseConverter implements Converter<List<Expression>, List<FromTypeExpression>> {
  private final OneOfAccessor<Expression, ValueCase> expressionAccessor;
  private final Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;
  private final EntityAttributeMapping entityAttributeMapping;
  private final AggregationColumnProvider aggregationColumnProvider;

  @Override
  public List<FromTypeExpression> convert(
      final List<Expression> expressions, final RequestContext requestContext)
      throws ConversionException {
    final Set<FromTypeExpression> set = new HashSet<>();

    for (final Expression expression : expressions) {
      final Optional<FromTypeExpression> optionalExpression = convert(expression, requestContext);
      optionalExpression.ifPresent(set::add);
    }

    return set.stream().collect(toUnmodifiableList());
  }

  private Optional<FromTypeExpression> convert(
      final Expression expression, final RequestContext requestContext) throws ConversionException {
    final ColumnIdentifier identifier;

    if (expression.getValueCase() == FUNCTION) {
      final Function aggregation = expressionAccessor.access(expression, FUNCTION);
      identifier = aggregationColumnProvider.getColumnIdentifier(aggregation);
    } else {
      identifier =
          expressionAccessor.access(
              expression, expression.getValueCase(), Set.of(COLUMNIDENTIFIER));
    }

    if (!entityAttributeMapping.isMultiValued(requestContext, identifier.getColumnName())) {
      return empty();
    }

    final IdentifierExpression identifierExpression =
        identifierExpressionConverter.convert(identifier, requestContext);
    final FromTypeExpression fromTypeExpression = UnnestExpression.of(identifierExpression, false);

    return Optional.of(fromTypeExpression);
  }
}
