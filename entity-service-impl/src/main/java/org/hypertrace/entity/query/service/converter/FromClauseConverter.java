package org.hypertrace.entity.query.service.converter;

import static java.util.Optional.empty;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;

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
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class FromClauseConverter implements Converter<List<Expression>, List<FromTypeExpression>> {
  private final OneOfAccessor<Expression, ValueCase> expressionAccessor;
  private final Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;
  private final EntityAttributeMapping entityAttributeMapping;

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
    final ColumnIdentifier identifier =
        expressionAccessor.access(expression, expression.getValueCase(), Set.of(COLUMNIDENTIFIER));

    if (!entityAttributeMapping.isMultiValued(requestContext, identifier.getColumnName())) {
      return empty();
    }

    final IdentifierExpression identifierExpression =
        identifierExpressionConverter.convert(identifier, requestContext);
    final FromTypeExpression fromTypeExpression = UnnestExpression.of(identifierExpression, false);

    return Optional.of(fromTypeExpression);
  }
}
