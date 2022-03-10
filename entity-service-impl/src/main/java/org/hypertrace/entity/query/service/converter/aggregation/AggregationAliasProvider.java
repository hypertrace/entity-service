package org.hypertrace.entity.query.service.converter.aggregation;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.entity.query.service.converter.AliasProvider;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Function;
import org.hypertrace.entity.service.util.StringUtils;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class AggregationAliasProvider implements AliasProvider<Function> {
  private final AliasProvider<ColumnIdentifier> identifierAliasProvider;
  private final AggregationColumnProvider aggregationColumnProvider;

  @Override
  public String getAlias(final Function aggregateExpression) throws ConversionException {
    final ColumnIdentifier containingIdentifier =
        aggregationColumnProvider.getColumnIdentifier(aggregateExpression);
    final String alias = aggregateExpression.getAlias();

    if (StringUtils.isNotBlank(alias)) {
      return alias;
    }

    return aggregateExpression.getFunctionName()
        + ALIAS_SEPARATOR
        + identifierAliasProvider.getAlias(containingIdentifier);
  }
}
