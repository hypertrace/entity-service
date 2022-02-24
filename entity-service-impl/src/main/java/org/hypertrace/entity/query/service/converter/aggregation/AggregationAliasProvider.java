package org.hypertrace.entity.query.service.converter.aggregation;

import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.List;
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
  private final OneOfAccessor<Expression, ValueCase> expressionAccessor;

  @Override
  public String getAlias(final Function aggregateExpression) throws ConversionException {
    final List<Expression> innerExpressions = aggregateExpression.getArgumentsList();

    if (innerExpressions.size() != 1) {
      throw new ConversionException("Aggregation function should have exactly one argument");
    }

    final Expression innerExpression = innerExpressions.get(0);
    final ColumnIdentifier containingIdentifier =
        expressionAccessor.access(
            innerExpression, innerExpression.getValueCase(), Set.of(COLUMNIDENTIFIER));
    final String alias = aggregateExpression.getAlias();

    if (StringUtils.isNotBlank(alias)) {
      return alias;
    }

    return aggregateExpression.getFunctionName()
        + ALIAS_SEPARATOR
        + identifierAliasProvider.getAlias(containingIdentifier);
  }
}
