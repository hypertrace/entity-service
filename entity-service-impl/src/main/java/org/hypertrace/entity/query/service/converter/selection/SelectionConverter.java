package org.hypertrace.entity.query.service.converter.selection;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.query.Selection;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.AliasProvider;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class SelectionConverter implements Converter<List<Expression>, Selection> {
  private final SelectionFactory selectionFactory;
  private final OneOfAccessor<Expression, ValueCase> expressionAccessor;

  @Override
  public Selection convert(final List<Expression> expressions, final RequestContext requestContext)
      throws ConversionException {
    final List<SelectionSpec> specs = new ArrayList<>();

    for (final Expression expression : expressions) {
      final SelectionSpec selectingSpec = getSpec(expression, requestContext);
      specs.add(selectingSpec);
    }

    return Selection.builder().selectionSpecs(specs).build();
  }

  private SelectionSpec getSpec(final Expression expression, final RequestContext requestContext)
      throws ConversionException {
    final ValueCase valueCase = expression.getValueCase();
    return getSpec(valueCase, expressionAccessor.access(expression, valueCase), requestContext);
  }

  private <T> SelectionSpec getSpec(
      final ValueCase valueCase, final T innerExpression, final RequestContext requestContext)
      throws ConversionException {
    final Converter<T, ? extends SelectingExpression> converter =
        selectionFactory.getConverter(valueCase);
    final AliasProvider<T> aliasProvider = selectionFactory.getAliasProvider(valueCase);

    final SelectingExpression selectingExpression =
        converter.convert(innerExpression, requestContext);
    final String alias = aliasProvider.getAlias(innerExpression);

    return SelectionSpec.of(selectingExpression);
  }
}
