package org.hypertrace.entity.query.service.converter.aggregation;

import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.hypertrace.entity.query.service.converter.AliasProvider;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.Function;
import org.hypertrace.entity.service.util.StringUtils;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class AggregationAliasProvider implements AliasProvider<Function> {
  private final AliasProvider<ColumnIdentifier> identifierAliasProvider;
  private final AggregationColumnProvider aggregationColumnProvider;
  private final OneOfAccessor<Expression, ValueCase> expressionAccessor;

  @Override
  public String getAlias(final Function aggregateExpression) throws ConversionException {
    final Expression expression =
        aggregationColumnProvider.getAggregationColumn(aggregateExpression);
    final ColumnIdentifier containingIdentifier =
        expressionAccessor.access(expression, expression.getValueCase(), Set.of(COLUMNIDENTIFIER));
    final String alias = aggregateExpression.getAlias();

    if (StringUtils.isNotBlank(alias)) {
      return alias;
    }

    return aggregateExpression.getFunctionName()
        + ALIAS_SEPARATOR
        + identifierAliasProvider.getAlias(containingIdentifier);
  }
}
