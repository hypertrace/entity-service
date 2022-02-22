package org.hypertrace.entity.query.service.converter.aggregation;

import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.GroupTypeExpression;
import org.hypertrace.core.documentstore.query.Aggregation;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class GroupByConverter implements Converter<List<Expression>, Aggregation> {
  private final OneOfAccessor<Expression, ValueCase> expressionAccessor;
  private final Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;

  @Override
  public Aggregation convert(
      final List<Expression> Expressions, final RequestContext requestContext)
      throws ConversionException {
    final List<GroupTypeExpression> groupTypeExpressions = new ArrayList<>();

    for (final Expression groupBy : Expressions) {
      final GroupTypeExpression groupTypeExpression = convert(groupBy, requestContext);
      groupTypeExpressions.add(groupTypeExpression);
    }

    return Aggregation.builder().expressions(groupTypeExpressions).build();
  }

  private GroupTypeExpression convert(final Expression groupBy, final RequestContext requestContext)
      throws ConversionException {
    final ColumnIdentifier identifier =
        expressionAccessor.access(groupBy, groupBy.getValueCase(), Set.of(COLUMNIDENTIFIER));

    return identifierExpressionConverter.convert(identifier, requestContext);
  }
}
