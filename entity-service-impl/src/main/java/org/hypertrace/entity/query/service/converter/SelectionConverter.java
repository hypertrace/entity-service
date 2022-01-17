package org.hypertrace.entity.query.service.converter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.query.Selection;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class SelectionConverter implements Converter<List<Expression>, Selection> {
  private final Converter<org.hypertrace.entity.query.service.v1.AggregateExpression, AggregateExpression> aggregateExpressionConverter;
  private final Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;

  private final AliasProvider<org.hypertrace.entity.query.service.v1.AggregateExpression> aggregateAliasProvider;
  private final AliasProvider<ColumnIdentifier> identifierAliasProvider;

  @Override
  public Selection convert(final List<Expression> expressions) throws ConversionException {
    final List<SelectionSpec> specs = new ArrayList<>();

    for (final Expression expression : expressions) {
      specs.add(getSpec(expression));
    }

    return Selection.builder().selectionSpecs(specs).build();
  }

  private SelectionSpec getSpec(final Expression expression) throws ConversionException {
    switch (expression.getValueCase()) {
      case AGGREGATION:
        return getSpec(expression.getAggregation(), aggregateExpressionConverter, aggregateAliasProvider);

      case COLUMNIDENTIFIER:
        return getSpec(expression.getColumnIdentifier(), identifierExpressionConverter, identifierAliasProvider);

      default:
        throw new ConversionException(String.format("Only identifier/aggregate selection is supported. Found: %s", expression));
    }
  }

  private <T> SelectionSpec getSpec(final T expression, final Converter<T, ? extends SelectingExpression> converter, final AliasProvider<T> aliasProvider)
      throws ConversionException {
    final SelectingExpression selectingExpression = converter.convert(expression);
    final String alias = aliasProvider.getAlias(expression);

    return SelectionSpec.of(selectingExpression, alias);
  }
}
