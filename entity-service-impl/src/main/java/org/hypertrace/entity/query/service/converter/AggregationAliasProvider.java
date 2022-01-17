package org.hypertrace.entity.query.service.converter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.entity.query.service.v1.AggregateExpression;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.service.util.StringUtils;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class AggregationAliasProvider implements AliasProvider<AggregateExpression> {
  private final AliasProvider<ColumnIdentifier> identifierAliasProvider;

  @Override
  public String getAlias(final AggregateExpression aggregateExpression) throws ConversionException {
    final Expression innerExpression = aggregateExpression.getExpression();

    if (!innerExpression.hasColumnIdentifier()) {
      throw new ConversionException(String.format("Column identifier expected in: %s", aggregateExpression));
    }

    final ColumnIdentifier containingIdentifier = innerExpression.getColumnIdentifier();
    final String alias = identifierAliasProvider.getAlias(containingIdentifier);

    if (StringUtils.isNotBlank(alias)) {
      return alias;
    }

    return aggregateExpression.getOperator() + ALIAS_SEPARATOR + containingIdentifier.getColumnName();
  }
}
