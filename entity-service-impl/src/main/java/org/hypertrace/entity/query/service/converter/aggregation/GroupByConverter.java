package org.hypertrace.entity.query.service.converter.aggregation;

import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;
import org.hypertrace.core.documentstore.query.Aggregation;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.converter.accessor.IOneOfAccessor;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.GroupByExpression;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class GroupByConverter implements Converter<List<GroupByExpression>, Aggregation> {
  private final IOneOfAccessor<Expression, ValueCase> expressionAccessor;
  private final Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;

  @Override
  public Aggregation convert(final List<GroupByExpression> groupByExpressions)
      throws ConversionException {
    final List<GroupingExpression> groupingExpressions = new ArrayList<>();

    for (final GroupByExpression groupBy : groupByExpressions) {
      groupingExpressions.add(convert(groupBy));
    }

    return Aggregation.builder().expressions(groupingExpressions).build();
  }

  private GroupingExpression convert(final GroupByExpression groupBy) throws ConversionException {
    final Expression innerExpression = groupBy.getExpression();

    final ColumnIdentifier identifier =
        expressionAccessor.access(
            innerExpression, innerExpression.getValueCase(), Set.of(COLUMNIDENTIFIER));

    return identifierExpressionConverter.convert(identifier);
  }
}
