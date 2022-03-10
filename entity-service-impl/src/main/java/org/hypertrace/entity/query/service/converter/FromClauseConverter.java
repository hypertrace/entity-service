package org.hypertrace.entity.query.service.converter;

import static java.util.Collections.unmodifiableList;
import static java.util.Optional.empty;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;

import java.util.ArrayList;
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
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class FromClauseConverter implements Converter<List<Expression>, List<FromTypeExpression>> {
  private final OneOfAccessor<Expression, ValueCase> expressionAccessor;
  private final Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;
  private final EntityAttributeMapping entityAttributeMapping;

  @Override
  public List<FromTypeExpression> convert(final List<Expression> groupBys, final RequestContext requestContext)
      throws ConversionException {
    final List<FromTypeExpression> list = new ArrayList<>();

    for (final Expression groupBy : groupBys) {
      final Optional<FromTypeExpression> optionalExpression = convert(groupBy, requestContext);
      optionalExpression.ifPresent(list::add);
    }

    return unmodifiableList(list);
  }

  private Optional<FromTypeExpression> convert(final Expression groupBy, final RequestContext requestContext)
      throws ConversionException {
    final ColumnIdentifier identifier =
        expressionAccessor.access(groupBy, groupBy.getValueCase(), Set.of(COLUMNIDENTIFIER));

    if (!entityAttributeMapping.isMultiValued(requestContext, identifier.getColumnName())) {
      return empty();
    }

    final IdentifierExpression identifierExpression = identifierExpressionConverter.convert(identifier, requestContext);
    final FromTypeExpression fromTypeExpression = UnnestExpression.of(identifierExpression, false);

    return Optional.of(fromTypeExpression);
  }
}
